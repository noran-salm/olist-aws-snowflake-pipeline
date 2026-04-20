-- generate_schema_name: always use the custom schema directly (no prefix)
-- Without this, dbt combines target.schema + custom_schema → STAGING_MARTS (wrong)
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}

-- safe_divide: null-safe division
{% macro safe_divide(numerator, denominator) %}
    IFF({{ denominator }} = 0, NULL, {{ numerator }} / {{ denominator }})
{% endmacro %}

-- date_spine_months: generate year-month series
{% macro date_spine_months(start_date, end_date) %}
    WITH date_spine AS (
        SELECT
            DATEADD('month', SEQ4(), '{{ start_date }}'::DATE) AS month_date
        FROM TABLE(GENERATOR(ROWCOUNT => 60))
        WHERE month_date <= '{{ end_date }}'::DATE
    )
    SELECT month_date, TO_CHAR(month_date, 'YYYY-MM') AS year_month
    FROM date_spine
{% endmacro %}
