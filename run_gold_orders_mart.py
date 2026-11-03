"""Pipeline simulating a Gold mart transformation with DuckDB and OpenLineage."""

import duckdb
from libs.duckdb_lineage import DuckDBLineageRunner

def main() -> None:
    runner = DuckDBLineageRunner()

    # Step 1: Temporarily connect to resolve the latest snapshot pointer
    con = runner._init_connection()
    orders_table_s3 = "s3://lakehouse-dev-silver/sales_silver/orders"
    latest_snapshot = runner.resolve_latest_iceberg_snapshot(con, orders_table_s3)
    print(f"Using resolved Iceberg snapshot:\n   {latest_snapshot}\n")

    # Step 2: Define the Gold business transformation (aggregate orders by status)
    gold_sql_query = f"""
        SELECT 
            status,
            COUNT(*) AS total_orders,
            MIN(order_date) AS earliest_order,
            MAX(order_date) AS latest_order
        FROM iceberg_scan('{latest_snapshot}')
        GROUP BY status
        ORDER BY total_orders DESC;
    """

    # Step 3: Execute query with automatic lineage capture
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

        logger.info(f"🚀 Starting job [{job_name}] (Run ID: {run_id})")

        start_time = datetime.now(timezone.utc)
        try:
            con = self._init_connection()
            result = con.sql(query)
            logger.info(f"Query executed successfully for job [{job_name}].")
        except Exception as exc:
            status = "FAIL"
            error_message = str(exc)
            logger.error(f"❌ Query execution failed for job [{job_name}]: {error_message}")
        end_time = datetime.now(timezone.utc)

        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        # Construct OpenLineage standard payload
        openlineage_payload = {
            "eventType": status,
            "eventTime": end_time.isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "errorMessage": {"message": error_message} if error_message else {},
                    "performance": {"executionTimeMs": duration_ms},
                },
            },
            "job": {
                "namespace": self.job_namespace,
                "name": job_name,
            },
            "inputs": [{"namespace": "iceberg", "name": table} for table in input_tables],
            "outputs": [{"namespace": "iceberg", "name": table} for table in output_tables],
        }

        # Save to local file for visual inspection and audit
        import json
        with open("latest_lineage_event.json", "w", encoding="utf-8") as f:
            json.dump(openlineage_payload, f, indent=2)
        logger.info("💾 Saved OpenLineage payload to 'latest_lineage_event.json'")

        # Emit Lineage event to OpenMetadata / OpenLineage
        self._send_lineage(openlineage_payload)

        if status == "FAIL":
            raise RuntimeError(f"Job [{job_name}] failed: {error_message}")

        return result

        print("\nGold Mart Results:")
        result.show()

if __name__ == "__main__":
    main()