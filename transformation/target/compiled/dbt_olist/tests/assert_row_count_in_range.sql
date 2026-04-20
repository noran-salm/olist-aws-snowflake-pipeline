-- tests/assert_row_count_in_range.sql
-- Fails if fct_orders row count is outside expected range
-- Catches: duplicate loads, truncation bugs, empty runs




WITH counts AS (
    SELECT COUNT(*) AS row_count
    FROM OLIST_DW.MARTS.fct_orders
)
SELECT row_count
FROM counts
WHERE row_count < 100000
   OR row_count > 200000
-- Returns rows if OUTSIDE range → test FAILS