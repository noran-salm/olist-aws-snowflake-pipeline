-- ─────────────────────────────────────────
-- 1. Create the REPORTER role (as ACCOUNTADMIN)
-- ─────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS REPORTER;

-- Grant minimal usage on database and marts schema
GRANT USAGE ON DATABASE OLIST_DW TO ROLE REPORTER;
GRANT USAGE ON SCHEMA OLIST_DW.MARTS TO ROLE REPORTER;

-- Optional: grant SELECT on future tables in MARTS to REPORTER
GRANT SELECT ON FUTURE TABLES IN SCHEMA OLIST_DW.MARTS TO ROLE REPORTER;

-- ─────────────────────────────────────────
-- 2. Now create schemas and grant ALL as SYSADMIN
-- ─────────────────────────────────────────
USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS OLIST_DW.MARTS;
CREATE SCHEMA IF NOT EXISTS OLIST_DW.STAGING;

-- SYSADMIN already owns the schemas (because it created them)
-- But explicitly grant ALL to be safe:
GRANT ALL ON SCHEMA OLIST_DW.MARTS   TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA OLIST_DW.STAGING TO ROLE SYSADMIN;