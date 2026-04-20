-- tests/assert_no_null_year_month.sql
-- Catches the Spark partition column issue we fixed earlier

SELECT COUNT(*) AS null_count
FROM {{ ref('fct_orders') }}
WHERE order_year_month IS NULL
HAVING COUNT(*) > 0
