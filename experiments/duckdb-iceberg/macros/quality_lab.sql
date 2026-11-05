{% macro prepare_quality_candidate_02() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

    {% do run_query("""
    CREATE TABLE
    polaris_catalog.duckdb_feasibility_lab.quality_candidate_02 AS
    SELECT *
    FROM (VALUES ('A', 5), ('B', -1))
         AS t(customer_id, order_count)
""") %}

{% do log("Deuxième candidat préparé", info=true) %}

{% endmacro %}


{% macro fix_quality_candidate_02() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

{% do run_query("""
    UPDATE polaris_catalog.duckdb_feasibility_lab.quality_candidate_02
    SET order_count = 3
    WHERE customer_id = 'B'
""") %}

{% do log("Candidat corrigé, sans publication", info=true) %}

{% endmacro %}

{% macro publish_quality_candidate_02() %}

    {% do run_query("DETACH polaris_catalog") %}

    {% do run_query("""
        ATTACH 'lakehouse_dev' AS polaris_catalog (
            TYPE iceberg,
            SECRET lab_polaris,
            ENDPOINT 'http://localhost:18181/api/catalog',
            ACCESS_DELEGATION_MODE 'none'
        )
    """) %}

{% set invalid_rows = run_query("""
    SELECT customer_id, order_count
    FROM polaris_catalog.duckdb_feasibility_lab.quality_candidate_02
    WHERE order_count < 0
       OR order_count IS NULL
""") %}

{% if execute %}
    {% if invalid_rows.rows | length > 0 %}
        {% do exceptions.raise_compiler_error(
            "Qualité refusée : candidat invalide"
        ) %}
    {% else %}
        {% do log(
            "Contrôle qualité réussi : aucune ligne invalide",
            info=true
        ) %}
    {% endif %}
{% endif %}

{% do run_query("""
    BEGIN TRANSACTION;

    ALTER TABLE
    polaris_catalog.duckdb_feasibility_lab.quality_published_01
    RENAME TO quality_previous_01;

    ALTER TABLE
    polaris_catalog.duckdb_feasibility_lab.quality_candidate_02
    RENAME TO quality_published_01;

    COMMIT;
""") %}

{% do log("Candidat validé puis publié", info=true) %}

{% endmacro %}
