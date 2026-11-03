"""Heavy analytical workload stress-test with multi-table OpenLineage tracking."""

import time
from libs.duckdb_lineage import DuckDBLineageRunner

def main() -> None:
    runner = DuckDBLineageRunner()
    con = runner._init_connection()

    # 1. Resolve snapshots for two distinct Silver Iceberg tables
    orders_table_s3 = "s3://lakehouse-dev-silver/sales_silver/orders"
    accounts_table_s3 = "s3://lakehouse-dev-silver/sales_silver/accounts"

    print("🔍 Resolving Iceberg snapshots for multi-table join...")
    orders_snapshot = runner.resolve_latest_iceberg_snapshot(con, orders_table_s3)
    accounts_snapshot = runner.resolve_latest_iceberg_snapshot(con, accounts_table_s3)

    print(f"   - Orders:   {orders_snapshot}")
    print(f"   - Accounts: {accounts_snapshot}\n")

    # 2. Complex analytical query:
    # - Joins orders with accounts
    # - Cross-joins with a 100,000 generated row series to stress execution
    # - Computes window functions (RANK) and aggregations
    heavy_sql_query = f"""
        WITH raw_orders AS (
            SELECT * FROM iceberg_scan('{orders_snapshot}')
        ),
        raw_accounts AS (
            SELECT * FROM iceberg_scan('{accounts_snapshot}')
        ),
        synthetic_traffic AS (
            SELECT range AS traffic_id, (range % 12) + 1 AS mock_order_index
            FROM range(100000)
        )
        SELECT 
            o.status,
            a.account_id,
            COUNT(t.traffic_id) AS traffic_volume,
            COUNT(DISTINCT o.order_id) AS distinct_orders,
            RANK() OVER (PARTITION BY o.status ORDER BY COUNT(t.traffic_id) DESC) as status_rank
        FROM raw_orders o
        INNER JOIN raw_accounts a ON o.customer_id = a.account_id
        LEFT JOIN synthetic_traffic t ON 1=1
        GROUP BY o.status, a.account_id
        ORDER BY traffic_volume DESC
        LIMIT 5;
    """

    print("⚡ Executing heavy query with multi-table lineage tracking...")
    start = time.perf_counter()

    result = runner.execute_with_lineage(
        query=heavy_sql_query,
        job_name="heavy_silver_to_gold_customer_traffic_mart",
        input_tables=["sales_silver.orders", "sales_silver.accounts"],
        output_tables=["customer_health_gold.customer_traffic_ranking"],
    )

    elapsed = (time.perf_counter() - start) * 1000
    print(f"\nTotal end-to-end execution time: {elapsed:.2f} ms")

    print("\n📊 Heavy Query Analytical Result:")
    result.show()

if __name__ == "__main__":
    main()