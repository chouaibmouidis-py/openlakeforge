{% macro probe_iceberg() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

{% set data = run_query("""
    SELECT customer_id, order_count
    FROM polaris_catalog.duckdb_feasibility_lab.dbt_customer_summary
    ORDER BY customer_id
""") %}

{% if execute %}
    {% for row in data.rows %}
        {% do log(row[0] ~ " : " ~ row[1], info=true) %}
    {% endfor %}
{% endif %}

{% endmacro %}


{% macro probe_rename_state() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

{% set result = run_query("""
    SELECT customer_id, order_count
    FROM polaris_catalog.duckdb_feasibility_lab.quality_published_01
    ORDER BY customer_id
""") %}

{% if execute %}
    {% for row in result.rows %}
        {% do log(row[0] ~ " : " ~ row[1], info=true) %}
    {% endfor %}
{% endif %}

{% endmacro %}

{% macro probe_quality_previous() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

{% set result = run_query("""
    SELECT customer_id, order_count
    FROM polaris_catalog.duckdb_feasibility_lab.quality_previous_01
    ORDER BY customer_id
""") %}

{% if execute %}
    {% for row in result.rows %}
        {% do log(row[0] ~ " : " ~ row[1], info=true) %}
    {% endfor %}
{% endif %}

{% endmacro %}
