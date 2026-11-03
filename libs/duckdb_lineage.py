"""DuckDB Lineage Runner for OpenLakeForge.

Executes DuckDB SQL queries over Iceberg tables stored in SeaweedFS, captures
execution errors, and emits OpenLineage events to OpenMetadata.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DuckDBLineage")


class DuckDBLineageRunner:
    """Executes SQL queries with automatic S3 configuration and OpenLineage tracking."""

    _METADATA_NAME = re.compile(r"/(\d+)-[^/]+\.metadata\.json$")
    _PRODUCER = "https://openlakeforge.dev/duckdb-lineage"

    def __init__(
        self,
        openlineage_url: str | None = None,
        job_namespace: str = "openlakeforge-local",
        s3_endpoint: str | None = None,
        s3_key: str | None = None,
        s3_secret: str | None = None,
    ) -> None:
        self.openlineage_url = openlineage_url or os.getenv(
            "OPENLINEAGE_URL", "http://localhost:8585/api/v1/openlineage/lineage"
        )
        self.job_namespace = job_namespace
        self.dataset_namespace = os.getenv("OPENLINEAGE_DATASET_NAMESPACE", "iceberg")
        self.local_event_path = os.getenv(
            "OPENLINEAGE_LOCAL_EVENT_PATH", "latest_lineage_event.json"
        )
        self.s3_endpoint = s3_endpoint or os.getenv(
            "OPENLAKEFORGE_STORAGE_ENDPOINT", "http://localhost:8333"
        )
        self.s3_key = s3_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.s3_secret = s3_secret or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.catalog_uri = os.getenv("OPENLAKEFORGE_CATALOG_REST_URI")
        self.catalog_warehouse = os.getenv(
            "OPENLAKEFORGE_CATALOG_WAREHOUSE", os.getenv("POLARIS_WAREHOUSE", "")
        )
        self.catalog_alias = os.getenv("DUCKDB_ICEBERG_CATALOG_ALIAS", "polaris_catalog")

    @staticmethod
    def _sql_literal(value: str) -> str:
        """Escape a value used in DuckDB's CREATE SECRET statement."""
        return value.replace("'", "''")

    def _require_storage_credentials(self) -> tuple[str, str]:
        if not self.s3_key or not self.s3_secret:
            raise RuntimeError(
                "DuckDB S3 credentials are missing; set AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY or pass them explicitly."
            )
        return self.s3_key, self.s3_secret

    def _attach_catalog(self, con: duckdb.DuckDBPyConnection) -> None:
        """Attach Polaris when catalog settings are supplied by the environment."""
        if not self.catalog_uri:
            return
        if not self.catalog_warehouse:
            raise RuntimeError(
                "OPENLAKEFORGE_CATALOG_WAREHOUSE is required when "
                "OPENLAKEFORGE_CATALOG_REST_URI is set."
            )
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.catalog_alias):
            raise ValueError(f"Invalid DuckDB catalog alias: {self.catalog_alias!r}")

        token = os.getenv("OPENLAKEFORGE_CATALOG_TOKEN") or os.getenv("POLARIS_ACCESS_TOKEN")
        client_id = os.getenv("POLARIS_CLIENT_ID") or os.getenv("POLARIS_FLOE_CLIENT_ID")
        client_secret = os.getenv("POLARIS_CLIENT_SECRET") or os.getenv("POLARIS_FLOE_CLIENT_SECRET")
        oauth_uri = os.getenv("OPENLAKEFORGE_CATALOG_TOKEN_URI") or os.getenv("POLARIS_TOKEN_URI")
        if token:
            con.sql(
                "CREATE SECRET iceberg_catalog_secret (TYPE iceberg, TOKEN "
                f"'{self._sql_literal(token)}');"
            )
        elif client_id and client_secret and oauth_uri:
            con.sql(
                "CREATE SECRET iceberg_catalog_secret (TYPE iceberg, "
                f"CLIENT_ID '{self._sql_literal(client_id)}', "
                f"CLIENT_SECRET '{self._sql_literal(client_secret)}', "
                f"OAUTH2_SERVER_URI '{self._sql_literal(oauth_uri)}');"
            )
        else:
            raise RuntimeError(
                "Polaris credentials are missing; set a catalog token or "
                "POLARIS_CLIENT_ID, POLARIS_CLIENT_SECRET and POLARIS_TOKEN_URI."
            )

        con.sql(
            f"ATTACH '{self._sql_literal(self.catalog_warehouse)}' AS {self.catalog_alias} "
            "(TYPE iceberg, SECRET iceberg_catalog_secret, "
            f"URI '{self._sql_literal(self.catalog_uri)}');"
        )

    def _init_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with required extensions and S3 secrets."""
        s3_key, s3_secret = self._require_storage_credentials()
        endpoint = self.s3_endpoint.removeprefix("http://").removeprefix("https://")
        use_ssl = self.s3_endpoint.startswith("https://")
        con = duckdb.connect()
        # Extensions are baked into the project-code image. Runtime INSTALL would
        # introduce an undeclared network dependency in an offline job pod.
        con.sql("LOAD httpfs;")
        con.sql("LOAD iceberg;")

        con.sql(f"""
        CREATE SECRET s3_seaweed (
            TYPE S3,
            KEY_ID '{self._sql_literal(s3_key)}',
            SECRET '{self._sql_literal(s3_secret)}',
            ENDPOINT '{self._sql_literal(endpoint)}',
            URL_STYLE 'path',
            USE_SSL {str(use_ssl).lower()}
        );
        """)
        self._attach_catalog(con)
        return con

    def resolve_latest_iceberg_snapshot(self, con: duckdb.DuckDBPyConnection, table_path: str) -> str:
        """Find Floe's highest numbered Iceberg metadata file in a table prefix.

        Floe names metadata files as ``00001-<uuid>.metadata.json``. The numeric
        sequence, rather than the UUID, is the version signal used here. This is
        deliberately scoped to that naming contract; it is not a generic
        replacement for resolving the current snapshot through an Iceberg catalog.
        """
        pattern = f"{table_path.rstrip('/')}/metadata/*.metadata.json"
        query = f"SELECT file FROM glob('{self._sql_literal(pattern)}');"
        rows = con.sql(query).fetchall()
        if not rows:
            raise FileNotFoundError(f"No Iceberg metadata files found for pattern: {pattern}")

        candidates = []
        for (file_path,) in rows:
            match = self._METADATA_NAME.search(file_path)
            if match:
                candidates.append((int(match.group(1)), file_path))
        if not candidates:
            raise ValueError(
                "No Floe-style numbered Iceberg metadata files found; resolve the "
                "current snapshot through the catalog instead."
            )
        return max(candidates, key=lambda candidate: candidate[0])[1]

    def execute_with_lineage(
        self,
        query: str,
        job_name: str,
        input_tables: list[str],
        output_tables: list[str] | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Execute query, capture metrics/errors and emit OpenLineage event."""
        output_tables = output_tables or []
        run_id = str(uuid.uuid4())
        status = "COMPLETE"
        error_message = None
        result = None

        logger.info(f"Starting job [{job_name}] (Run ID: {run_id})")

        try:
            con = self._init_connection()
            result = con.sql(query)
            logger.info(f"✅ Query executed successfully for job [{job_name}].")
        except Exception as exc:
            status = "FAIL"
            error_message = str(exc)
            logger.error(f"❌ Query execution failed for job [{job_name}]: {error_message}")

        # Construct OpenLineage standard payload
        openlineage_payload = {
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
            "producer": self._PRODUCER,
            "eventType": status,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
                "facets": {"errorMessage": {"message": error_message}} if error_message else {},
            },
            "job": {
                "namespace": self.job_namespace,
                "name": job_name,
            },
            "inputs": [{"namespace": self.dataset_namespace, "name": table} for table in input_tables],
            "outputs": [{"namespace": self.dataset_namespace, "name": table} for table in output_tables],
        }

        # Emit Lineage event to OpenMetadata / OpenLineage
        self._send_lineage(openlineage_payload)

        if status == "FAIL":
            raise RuntimeError(f"Job [{job_name}] failed: {error_message}")

        assert result is not None
        return result

    def _send_lineage(self, payload: dict[str, Any]) -> None:
        """Send OpenLineage payload via HTTP POST and save local copy."""
        # Save local JSON file for visual verification
        with open(self.local_event_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info("Saved OpenLineage payload to %s", self.local_event_path)

        try:
            headers = {"Content-Type": "application/json"}
            if api_key := os.getenv("OPENLINEAGE_API_KEY"):
                headers["Authorization"] = f"Bearer {api_key}"
            response = requests.post(self.openlineage_url, json=payload, headers=headers, timeout=3)
            if response.status_code in (200, 201, 202):
                logger.info("OpenLineage event successfully delivered.")
            else:
                logger.warning("OpenLineage returned HTTP %s: %s", response.status_code, response.text)
        except Exception as exc:
            logger.warning("Lineage collector unreachable (%s); continuing pipeline.", exc)
