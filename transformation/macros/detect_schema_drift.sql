-- macros/detect_schema_drift.sql
-- Call in dbt models to log unexpected columns

{% macro detect_schema_drift(expected_cols, source_relation) %}
    {% if execute %}
        {% set actual_cols = adapter.get_columns_in_relation(source_relation) %}
        {% set actual_names = actual_cols | map(attribute='name') | map('lower') | list %}
        {% set expected_lower = expected_cols | map('lower') | list %}

        {% set missing = [] %}
        {% set new_cols = [] %}

        {% for col in expected_lower %}
            {% if col not in actual_names %}
                {% do missing.append(col) %}
            {% endif %}
        {% endfor %}

        {% for col in actual_names %}
            {% if col not in expected_lower %}
                {% do new_cols.append(col) %}
            {% endif %}
        {% endfor %}

        {% if missing %}
            {{ exceptions.raise_compiler_error(
                "SCHEMA DRIFT: Missing required columns: " ~ missing | join(", ")
            ) }}
        {% endif %}

        {% if new_cols %}
            {{ log("SCHEMA INFO: New columns detected (additive): " ~ new_cols, info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
