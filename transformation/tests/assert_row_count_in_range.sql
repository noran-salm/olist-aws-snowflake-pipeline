-- tests/assert_row_count_in_range.sql
-- Fails if fct_orders row count is outside expected range
-- Catches: duplicate loads, truncation bugs, empty runs

{% set min_rows = 100000 %}
{% set max_rows = 200000 %}

WITH counts AS (
    SELECT COUNT(*) AS row_count
    FROM {{ ref('fct_orders') }}
)
SELECT row_count
FROM counts
WHERE row_count < {{ min_rows }}
   OR row_count > {{ max_rows }}
-- Returns rows if OUTSIDE range → test FAILS
