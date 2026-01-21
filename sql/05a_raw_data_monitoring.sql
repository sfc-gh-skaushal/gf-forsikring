/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 05a: Raw Data Quality Monitoring Setup
================================================================================
Purpose: Set up data quality monitoring on RAW_CLAIMS and RAW_POLICIES tables
         using system DMFs and custom DMFs
Author: Demo Setup Script
Date: 2025-01

Based on Snowflake Data Quality Monitoring best practices
================================================================================
*/

/*
******************************************************************************
* SECTION 1: ACCESS CONTROL SETUP
******************************************************************************
*/

-- Run as ACCOUNTADMIN for full privileges to set up DMFs
USE ROLE ACCOUNTADMIN;

-- Grant execute data metric function privilege to DATA_ENGINEER
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE DATA_ENGINEER;

-- Grant the data_metric_user database role for viewing DMF results
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_ENGINEER;

-- Grant privileges on GOVERNANCE schema for creating custom DMFs
GRANT USAGE ON DATABASE INSURANCECO TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE DATA_ENGINEER;
GRANT CREATE DATA METRIC FUNCTION ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE DATA_ENGINEER;

-- Grant privileges on RAW schema for DATA_ENGINEER to view data
GRANT USAGE ON SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;

/*
******************************************************************************
* SECTION 2: SET DATA METRIC SCHEDULE ON RAW TABLES
******************************************************************************
*/

-- Continue as ACCOUNTADMIN (has full privileges to modify tables and add DMFs)
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;
USE SCHEMA RAW;

-- Define schedule on RAW_CLAIMS (runs every 60 minutes)
ALTER TABLE RAW_CLAIMS SET DATA_METRIC_SCHEDULE = '60 MINUTE';

-- Define schedule on RAW_POLICIES (runs every 60 minutes)
ALTER TABLE RAW_POLICIES SET DATA_METRIC_SCHEDULE = '60 MINUTE';

/*
******************************************************************************
* SECTION 3: ADD SYSTEM DMF ASSOCIATIONS TO RAW_CLAIMS
******************************************************************************
*/

-- ROW_COUNT: Monitor volume changes
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- FRESHNESS: Monitor data staleness using _loaded_at timestamp
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (_loaded_at);

-- NULL_COUNT: Monitor NULL values in critical columns
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_id);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_id);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_amount);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (date_of_incident);

-- DUPLICATE_COUNT: Check for duplicate claim IDs
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (claim_id);

-- UNIQUE_COUNT: Track unique values for cardinality
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_type);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_status);

/*
******************************************************************************
* SECTION 4: ADD SYSTEM DMF ASSOCIATIONS TO RAW_POLICIES
******************************************************************************
*/

-- ROW_COUNT: Monitor volume changes
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- FRESHNESS: Monitor data staleness using _loaded_at timestamp
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (_loaded_at);

-- NULL_COUNT: Monitor NULL values in critical columns
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_id);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_holder_name);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (coverage_limit);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_start_date);

-- DUPLICATE_COUNT: Check for duplicate policy IDs
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (policy_id);

-- UNIQUE_COUNT: Track unique values
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (policy_type);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (risk_score);

/*
******************************************************************************
* SECTION 5: CREATE CUSTOM DMFs FOR RAW DATA VALIDATION
******************************************************************************
*/

USE SCHEMA INSURANCECO.GOVERNANCE;

-- DMF: Validate claim_amount is positive
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_CLAIM_AMOUNT(
    ARG_T TABLE(claim_amount NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts raw claims with invalid (negative or zero) claim amounts'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount IS NULL OR claim_amount <= 0
$$;

-- DMF: Validate date_reported is not before date_of_incident
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_DATES(
    ARG_T TABLE(date_of_incident DATE, date_reported DATE)
)
RETURNS NUMBER
COMMENT = 'Counts raw claims where report date is before incident date'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE date_reported < date_of_incident
$$;

-- DMF: Validate email format in raw data
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_EMAIL(
    ARG_T TABLE(policy_holder_email VARCHAR)
)
RETURNS NUMBER
COMMENT = 'Counts records with invalid email format in raw data'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_holder_email IS NOT NULL
      AND NOT REGEXP_LIKE(policy_holder_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
$$;

-- DMF: Validate CPR format (Danish personal ID: DDMMYY-XXXX)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_CPR(
    ARG_T TABLE(policy_holder_cpr VARCHAR)
)
RETURNS NUMBER
COMMENT = 'Counts records with invalid Danish CPR number format'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_holder_cpr IS NOT NULL
      AND NOT REGEXP_LIKE(policy_holder_cpr, '^[0-9]{6}-[0-9]{4}$')
$$;

-- DMF: Referential integrity - Claims should have valid policy_id in policies table
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_ORPHAN_CLAIMS(
    ARG_T1 TABLE(policy_id VARCHAR),
    ARG_T2 TABLE(policy_id VARCHAR)
)
RETURNS NUMBER
COMMENT = 'Counts claims with policy_id not found in policies table (orphan records)'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T1
    WHERE policy_id NOT IN (SELECT policy_id FROM ARG_T2 WHERE policy_id IS NOT NULL)
$$;

-- DMF: Validate coverage_limit is positive
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_COVERAGE(
    ARG_T TABLE(coverage_limit NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts policies with invalid (negative or zero) coverage limits'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE coverage_limit IS NULL OR coverage_limit <= 0
$$;

-- DMF: Validate policy dates (end should be after start)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_RAW_INVALID_POLICY_DATES(
    ARG_T TABLE(policy_start_date DATE, policy_end_date DATE)
)
RETURNS NUMBER
COMMENT = 'Counts policies where end date is before start date'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_end_date < policy_start_date
$$;

/*
******************************************************************************
* SECTION 6: APPLY CUSTOM DMFs TO RAW TABLES
******************************************************************************
*/

USE SCHEMA INSURANCECO.RAW;

-- Apply custom DMFs to RAW_CLAIMS
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_CLAIM_AMOUNT
    ON (claim_amount);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_DATES
    ON (date_of_incident, date_reported);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_EMAIL
    ON (policy_holder_email);

ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_CPR
    ON (policy_holder_cpr);

-- Apply custom DMFs to RAW_POLICIES
ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_EMAIL
    ON (policy_holder_email);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_CPR
    ON (policy_holder_cpr);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_COVERAGE
    ON (coverage_limit);

ALTER TABLE RAW_POLICIES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_INVALID_POLICY_DATES
    ON (policy_start_date, policy_end_date);

-- Apply referential integrity check (claims -> policies)
-- Note: This checks that every claim's policy_id exists in the policies table
ALTER TABLE RAW_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_RAW_ORPHAN_CLAIMS
    ON (policy_id, TABLE(INSURANCECO.RAW.RAW_POLICIES(policy_id)));

/*
******************************************************************************
* SECTION 7: VERIFY DMF SETUP
******************************************************************************
*/

-- Show all DMFs associated with RAW_CLAIMS
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.RAW.RAW_CLAIMS',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Show all DMFs associated with RAW_POLICIES
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.RAW.RAW_POLICIES',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Check current schedule on tables
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE RAW_CLAIMS;
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE RAW_POLICIES;

/*
******************************************************************************
* SECTION 8: QUERY DMF RESULTS
******************************************************************************
*/

-- View latest DMF results for RAW_CLAIMS
SELECT 
    --measurement_time,
    metric_name,
    table_name,
    --column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'RAW'
  AND table_name = 'RAW_CLAIMS'
--ORDER BY measurement_time DESC
LIMIT 20;

select * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;

-- View latest DMF results for RAW_POLICIES
SELECT 
    --measurement_time,
    metric_name,
    table_name,
   -- column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'RAW'
  AND table_name = 'RAW_POLICIES'
--ORDER BY measurement_time DESC
LIMIT 20;

-- Summary view of all data quality issues
SELECT 
    table_name,
    metric_name,
    MAX(value) AS latest_value
    --MAX(measurement_time) AS last_checked
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'RAW'
GROUP BY table_name, metric_name
HAVING MAX(value) > 0
ORDER BY table_name, metric_name;

/*
******************************************************************************
* SECTION 9: OPTIONAL - TRIGGER IMMEDIATE DMF EXECUTION
******************************************************************************
*/

-- To manually test a DMF, call it directly with SELECT:
-- SELECT SNOWFLAKE.CORE.ROW_COUNT(SELECT * FROM INSURANCECO.RAW.RAW_CLAIMS);
-- SELECT SNOWFLAKE.CORE.NULL_COUNT(SELECT claim_id FROM INSURANCECO.RAW.RAW_CLAIMS);

-- Note: Scheduled DMFs run automatically per DATA_METRIC_SCHEDULE (60 MINUTE).
-- To trigger on data changes instead, use: ALTER TABLE RAW_CLAIMS SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/*
******************************************************************************
* SUMMARY OF DMFs CONFIGURED
******************************************************************************

RAW_CLAIMS Table:
-----------------
System DMFs:
  - ROW_COUNT: Monitor volume
  - FRESHNESS: Monitor staleness (_loaded_at)
  - NULL_COUNT: claim_id, policy_id, claim_amount, date_of_incident
  - DUPLICATE_COUNT: claim_id
  - UNIQUE_COUNT: claim_type, claim_status

Custom DMFs:
  - DMF_RAW_INVALID_CLAIM_AMOUNT: Negative/zero amounts
  - DMF_RAW_INVALID_DATES: Report before incident
  - DMF_RAW_INVALID_EMAIL: Email format validation
  - DMF_RAW_INVALID_CPR: Danish CPR format validation
  - DMF_RAW_ORPHAN_CLAIMS: Referential integrity to policies

RAW_POLICIES Table:
-------------------
System DMFs:
  - ROW_COUNT: Monitor volume
  - FRESHNESS: Monitor staleness (_loaded_at)
  - NULL_COUNT: policy_id, policy_holder_name, coverage_limit, policy_start_date
  - DUPLICATE_COUNT: policy_id
  - UNIQUE_COUNT: policy_type, risk_score

Custom DMFs:
  - DMF_RAW_INVALID_EMAIL: Email format validation
  - DMF_RAW_INVALID_CPR: Danish CPR format validation
  - DMF_RAW_INVALID_COVERAGE: Negative/zero coverage
  - DMF_RAW_INVALID_POLICY_DATES: End before start date

Navigate to the table's Data Quality page in Snowsight to review results!
******************************************************************************
*/

SELECT 'Raw data monitoring setup complete!' AS STATUS,
       'RAW_CLAIMS: 11 DMFs configured' AS CLAIMS_DMFS,
       'RAW_POLICIES: 10 DMFs configured' AS POLICIES_DMFS,
       'Schedule: 60 MINUTE' AS SCHEDULE,
       'Navigate to Data Quality tab in Snowsight to view results' AS NEXT_STEP;
