"""DuckDB Lineage Runner for OpenLakeForge.

Executes DuckDB SQL queries over Iceberg tables stored in SeaweedFS, captures
execution errors, and emits OpenLineage events to OpenMetadata.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DuckDBLineage")


class DuckDBLineageRunner:
    """Executes SQL queries with automatic S3 configuration and OpenLineage tracking."""

    def __init__(
        self,
        openlineage_url: str | None = None,
        job_namespace: str = "openlakeforge-local",
        s3_endpoint: str = "localhost:8333",
        s3_key: str = "***REMOVED***",
        s3_secret: str = "***REMOVED***",
    ) -> None:
        self.openlineage_url = openlineage_url or os.getenv(
            "OPENLINEAGE_URL", "http://localhost:8585/api/v1/openlineage/lineage"
        )
        self.job_namespace = job_namespace
        self.s3_endpoint = s3_endpoint
        self.s3_key = s3_key
        self.s3_secret = s3_secret

    def _init_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with required extensions and S3 secrets."""
        con = duckdb.connect()
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("INSTALL iceberg; LOAD iceberg;")

        # Configure S3 secret for SeaweedFS
        con.sql(f"""
        CREATE SECRET s3_seaweed (
            TYPE S3,
            KEY_ID '{self.s3_key}',
            SECRET '{self.s3_secret}',
            ENDPOINT '{self.s3_endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        );
        """)
        return con

    def resolve_latest_iceberg_snapshot(self, con: duckdb.DuckDBPyConnection, table_path: str) -> str:
        """Find the latest Iceberg metadata JSON file in the given table prefix."""
        pattern = f"{table_path.rstrip('/')}/metadata/*.metadata.json"
        query = f"SELECT file FROM glob('{pattern}') ORDER BY file DESC LIMIT 1;"
        rows = con.sql(query).fetchall()
        if not rows:
            raise FileNotFoundError(f"No Iceberg metadata files found for pattern: {pattern}")
        return rows[0][0]

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
            "eventType": status,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "errorMessage": {"message": error_message} if error_message else {}
                },
            },
            "job": {
                "namespace": self.job_namespace,
                "name": job_name,
            },
            "inputs": [{"namespace": "iceberg", "name": table} for table in input_tables],
            "outputs": [{"namespace": "iceberg", "name": table} for table in output_tables],
        }

        # Emit Lineage event to OpenMetadata / OpenLineage
        self._send_lineage(openlineage_payload)

        if status == "FAIL":
            raise RuntimeError(f"Job [{job_name}] failed: {error_message}")

        return result

    def _send_lineage(self, payload: dict[str, Any]) -> None:
        """Send OpenLineage payload via HTTP POST and save local copy."""
        # Save local JSON file for visual verification
        with open("latest_lineage_event.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        logger.info("💾 Saved OpenLineage payload to 'latest_lineage_event.json'")

        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(self.openlineage_url, json=payload, headers=headers, timeout=3)
            if response.status_code in (200, 201, 202):
                logger.info("📡 OpenLineage event successfully delivered.")
            else:
                logger.warning(f"⚠️ OpenLineage returned HTTP {response.status_code}: {response.text}")
        except Exception as exc:
            logger.warning(f"⚠️ Lineage collector unreachable ({exc}); continuing pipeline.")