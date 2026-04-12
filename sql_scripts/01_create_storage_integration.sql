-- ============================================================
-- OLIST PIPELINE — Complete Snowflake Setup
-- Run as ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── 1. Storage Integration ────────────────────────────────────
CREATE STORAGE INTEGRATION IF NOT EXISTS olist_s3_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::516671521715:role/olist-snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://olist-lake-516671521715/processed/');

-- ── 2. Get the AWS principal Snowflake needs ──────────────────
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from output
DESC INTEGRATION olist_s3_integration;
