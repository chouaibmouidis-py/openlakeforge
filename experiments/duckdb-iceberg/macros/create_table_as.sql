{% macro duckdb__create_table_as(
    temporary, relation, compiled_code, language='sql'
) %}

    {% if target.name == 'iceberg'
          and relation.database == 'polaris_catalog'
          and relation.schema == 'duckdb_feasibility_lab'
          and not temporary
          and language == 'sql' %}

        {% set location =
            's3://lakehouse-dev-silver/duckdb_feasibility_lab/'
            ~ relation.identifier ~ '_' ~ invocation_id
        %}

        CREATE TABLE {{ relation }}
        WITH ('location' = '{{ location }}')
        AS (
            {{ compiled_code }}
        )

    {% else %}

        {{ return(dbt_duckdb.duckdb__create_table_as(
            temporary, relation, compiled_code, language
        )) }}

    {% endif %}

{% endmacro %}
