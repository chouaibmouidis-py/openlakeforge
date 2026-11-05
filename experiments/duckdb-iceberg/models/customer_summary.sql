{{ config(
    materialized='table',
    database='polaris_catalog',
    alias='dbt_customer_summary',
    pre_hook=[
        "DETACH polaris_catalog",
        """
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
        """
    ]
) }}

SELECT
    customer_id,
    COUNT(*) AS order_count
FROM (
    VALUES
        ('A', 101),
        ('A', 102),
        ('B', 103),
        ('A', 104),
        ('A', 105),
        ('B', 106)
) AS orders(customer_id, order_id)
GROUP BY customer_id
