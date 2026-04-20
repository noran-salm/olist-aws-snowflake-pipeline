-- Run in Snowflake once
USE ROLE SYSADMIN;
USE DATABASE OLIST_DW;
CREATE SCHEMA IF NOT EXISTS OLIST_DW.AUDIT;

CREATE TABLE IF NOT EXISTS OLIST_DW.AUDIT.pipeline_validation_log (
    run_id              VARCHAR(100),
    check_name          VARCHAR(200),
    check_stage         VARCHAR(50),   -- S3 | GLUE | DBT | SNOWFLAKE
    status              VARCHAR(20),   -- PASS | FAIL | WARN
    rows_checked        INTEGER,
    rows_failed         INTEGER,
    error_message       VARCHAR(2000),
    pipeline_run_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    duration_ms         INTEGER
);

-- View for monitoring
CREATE OR REPLACE VIEW OLIST_DW.AUDIT.latest_validation_summary AS
SELECT
    run_id,
    check_stage,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warnings,
    MAX(pipeline_run_ts) AS last_run
FROM OLIST_DW.AUDIT.pipeline_validation_log
GROUP BY 1, 2
ORDER BY last_run DESC;