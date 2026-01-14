/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 05b: Curated Data Quality Monitoring Setup
================================================================================
Purpose: Set up data quality monitoring on CURATED schema tables
         (DIM_CLAIMS, DIM_POLICIES) using system DMFs and custom DMFs
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

-- Run as ACCOUNTADMIN to grant required privileges
USE ROLE ACCOUNTADMIN;

-- Grant privileges on CURATED schema
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;
GRANT MODIFY ON ALL TABLES IN SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;

-- Grant privileges for viewing DMF results to other roles
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_ANALYST;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_SCIENTIST;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_STEWARD;

/*
******************************************************************************
* SECTION 2: SET DATA METRIC SCHEDULE ON CURATED TABLES
******************************************************************************
*/

-- Switch to DATA_ENGINEER role
USE ROLE DATA_ENGINEER;
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;
USE SCHEMA CURATED;

-- Define schedule on DIM_CLAIMS (runs every 60 minutes)
ALTER TABLE DIM_CLAIMS SET DATA_METRIC_SCHEDULE = '60 MINUTE';

-- Define schedule on DIM_POLICIES (runs every 60 minutes)
ALTER TABLE DIM_POLICIES SET DATA_METRIC_SCHEDULE = '60 MINUTE';

/*
******************************************************************************
* SECTION 3: ADD SYSTEM DMF ASSOCIATIONS TO DIM_CLAIMS
******************************************************************************
*/

-- ROW_COUNT: Monitor volume changes
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- FRESHNESS: Monitor data staleness using updated_at timestamp
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (updated_at);

-- NULL_COUNT: Monitor NULL values in critical columns
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_id);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_id);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_amount);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (date_of_incident);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_type);

-- DUPLICATE_COUNT: Check for duplicate claim IDs (should be 0 in curated)
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (claim_id);

-- UNIQUE_COUNT: Track unique values for cardinality monitoring
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_type);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_status);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (region);

/*
******************************************************************************
* SECTION 4: ADD SYSTEM DMF ASSOCIATIONS TO DIM_POLICIES
******************************************************************************
*/

-- ROW_COUNT: Monitor volume changes
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- FRESHNESS: Monitor data staleness using updated_at timestamp
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (updated_at);

-- NULL_COUNT: Monitor NULL values in critical columns
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_id);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_holder_name);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (coverage_limit);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (policy_type);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (risk_score);

-- DUPLICATE_COUNT: Check for duplicate policy IDs (should be 0 in curated)
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (policy_id);

-- UNIQUE_COUNT: Track unique values
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (policy_type);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (risk_score);

ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (region);

/*
******************************************************************************
* SECTION 5: CREATE CUSTOM DMFs FOR CURATED DATA VALIDATION
******************************************************************************
*/

USE SCHEMA INSURANCECO.GOVERNANCE;

-- DMF: Business rule - Claim amount must be positive
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_INVALID_CLAIM_AMOUNT(
    ARG_T TABLE(claim_amount NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts curated claims with invalid (negative or zero) claim amounts - CRITICAL if > 0'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount IS NULL OR claim_amount <= 0
$$;

-- DMF: Business rule - Coverage limit must be positive
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_INVALID_COVERAGE(
    ARG_T TABLE(policy_coverage_limit NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts claims with invalid coverage limit - CRITICAL if > 0'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_coverage_limit IS NULL OR policy_coverage_limit <= 0
$$;

-- DMF: Business rule - Claim amount should not exceed coverage limit
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_CLAIMS_EXCEEDING_COVERAGE(
    ARG_T TABLE(claim_amount NUMBER, policy_coverage_limit NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts claims where amount exceeds policy coverage - potential fraud or data error'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount > policy_coverage_limit
$$;

-- DMF: Business rule - Date reported should not be before incident date
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_INVALID_DATE_SEQUENCE(
    ARG_T TABLE(date_of_incident DATE, date_reported DATE)
)
RETURNS NUMBER
COMMENT = 'Counts claims with invalid date sequence - CRITICAL if > 0'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE date_reported < date_of_incident
$$;

-- DMF: Business rule - Days to report should be non-negative
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_NEGATIVE_DAYS_TO_REPORT(
    ARG_T TABLE(days_to_report NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts claims with negative days_to_report - indicates calculation error'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE days_to_report < 0
$$;

-- DMF: Monitor fraud flag rate
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_FRAUD_FLAG_RATE(
    ARG_T TABLE(fraud_flag BOOLEAN)
)
RETURNS NUMBER
COMMENT = 'Returns percentage of claims flagged for fraud - alert if unusually high'
AS
$$
    SELECT ROUND(
        (SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0)), 
        2
    )::NUMBER
    FROM ARG_T
$$;

-- DMF: Coverage utilization should be between 0 and 100 (or slightly over)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_INVALID_COVERAGE_UTIL(
    ARG_T TABLE(coverage_utilization_pct NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts claims with invalid coverage utilization (negative or NULL)'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE coverage_utilization_pct IS NULL OR coverage_utilization_pct < 0
$$;

-- DMF: High value claims count (over 100K DKK)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_HIGH_VALUE_CLAIMS(
    ARG_T TABLE(claim_amount NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts high-value claims over 100,000 DKK for monitoring'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount > 100000
$$;

-- DMF: Same-day claims (potential fraud pattern)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_SAME_DAY_CLAIMS(
    ARG_T TABLE(date_of_incident DATE, date_reported DATE)
)
RETURNS NUMBER
COMMENT = 'Counts claims reported same day as incident - may indicate fraud pattern'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE date_of_incident = date_reported
$$;

-- DMF: Policy active status validation
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_EXPIRED_POLICIES(
    ARG_T TABLE(is_active BOOLEAN, policy_end_date DATE)
)
RETURNS NUMBER
COMMENT = 'Counts policies marked active but with past end date - data inconsistency'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE is_active = TRUE AND policy_end_date < CURRENT_DATE()
$$;

-- DMF: Referential integrity - All claims should have valid policy
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CURATED_ORPHAN_CLAIMS(
    ARG_T1 TABLE(policy_id VARCHAR),
    ARG_T2 TABLE(policy_id VARCHAR)
)
RETURNS NUMBER
COMMENT = 'Counts claims with policy_id not found in DIM_POLICIES - referential integrity'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T1
    WHERE policy_id NOT IN (SELECT policy_id FROM ARG_T2 WHERE policy_id IS NOT NULL)
$$;

/*
******************************************************************************
* SECTION 6: APPLY CUSTOM DMFs TO DIM_CLAIMS
******************************************************************************
*/

USE SCHEMA INSURANCECO.CURATED;

-- Apply business rule DMFs to DIM_CLAIMS
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_INVALID_CLAIM_AMOUNT
    ON (claim_amount);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_INVALID_COVERAGE
    ON (policy_coverage_limit);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_CLAIMS_EXCEEDING_COVERAGE
    ON (claim_amount, policy_coverage_limit);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_INVALID_DATE_SEQUENCE
    ON (date_of_incident, date_reported);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_NEGATIVE_DAYS_TO_REPORT
    ON (days_to_report);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_FRAUD_FLAG_RATE
    ON (fraud_flag);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_INVALID_COVERAGE_UTIL
    ON (coverage_utilization_pct);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_HIGH_VALUE_CLAIMS
    ON (claim_amount);

ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_SAME_DAY_CLAIMS
    ON (date_of_incident, date_reported);

-- Apply referential integrity check
ALTER TABLE DIM_CLAIMS
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_ORPHAN_CLAIMS
    ON (policy_id, TABLE(INSURANCECO.CURATED.DIM_POLICIES(policy_id)));

/*
******************************************************************************
* SECTION 7: APPLY CUSTOM DMFs TO DIM_POLICIES
******************************************************************************
*/

-- Apply DMFs to DIM_POLICIES
ALTER TABLE DIM_POLICIES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CURATED_EXPIRED_POLICIES
    ON (is_active, policy_end_date);

/*
******************************************************************************
* SECTION 8: VERIFY DMF SETUP
******************************************************************************
*/

-- Show all DMFs associated with DIM_CLAIMS
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.CURATED.DIM_CLAIMS',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Show all DMFs associated with DIM_POLICIES
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.CURATED.DIM_POLICIES',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Check current schedule on tables
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE DIM_CLAIMS;
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE DIM_POLICIES;

/*
******************************************************************************
* SECTION 9: QUERY DMF RESULTS
******************************************************************************
*/

-- View latest DMF results for DIM_CLAIMS
SELECT 
    measurement_time,
    metric_name,
    table_name,
    column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
  AND table_name = 'DIM_CLAIMS'
ORDER BY measurement_time DESC
LIMIT 30;

-- View latest DMF results for DIM_POLICIES
SELECT 
    measurement_time,
    metric_name,
    table_name,
    column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
  AND table_name = 'DIM_POLICIES'
ORDER BY measurement_time DESC
LIMIT 20;

-- Data Quality Dashboard: Show all issues in CURATED schema
SELECT 
    table_name,
    metric_name,
    MAX(value) AS latest_value,
    MAX(measurement_time) AS last_checked,
    CASE 
        WHEN metric_name LIKE '%INVALID%' AND MAX(value) > 0 THEN 'CRITICAL'
        WHEN metric_name LIKE '%ORPHAN%' AND MAX(value) > 0 THEN 'CRITICAL'
        WHEN metric_name LIKE '%EXCEEDING%' AND MAX(value) > 0 THEN 'WARNING'
        WHEN metric_name = 'DUPLICATE_COUNT' AND MAX(value) > 0 THEN 'CRITICAL'
        WHEN metric_name = 'DMF_CURATED_FRAUD_FLAG_RATE' AND MAX(value) > 25 THEN 'WARNING'
        ELSE 'OK'
    END AS severity
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
GROUP BY table_name, metric_name
ORDER BY 
    CASE 
        WHEN metric_name LIKE '%INVALID%' AND MAX(value) > 0 THEN 1
        WHEN metric_name LIKE '%ORPHAN%' AND MAX(value) > 0 THEN 1
        WHEN MAX(value) > 0 THEN 2
        ELSE 3
    END,
    table_name, metric_name;

/*
******************************************************************************
* SECTION 10: OPTIONAL - TRIGGER IMMEDIATE DMF EXECUTION
******************************************************************************
*/

-- Uncomment to manually trigger DMF execution (for demo purposes)
-- EXECUTE DATA METRIC FUNCTION ON TABLE INSURANCECO.CURATED.DIM_CLAIMS;
-- EXECUTE DATA METRIC FUNCTION ON TABLE INSURANCECO.CURATED.DIM_POLICIES;

/*
******************************************************************************
* SUMMARY OF DMFs CONFIGURED
******************************************************************************

DIM_CLAIMS Table (21 DMFs):
---------------------------
System DMFs:
  - ROW_COUNT: Monitor volume
  - FRESHNESS: Monitor staleness (updated_at)
  - NULL_COUNT: claim_id, policy_id, claim_amount, date_of_incident, claim_type
  - DUPLICATE_COUNT: claim_id
  - UNIQUE_COUNT: claim_type, claim_status, region

Custom DMFs:
  - DMF_CURATED_INVALID_CLAIM_AMOUNT: Negative/zero amounts
  - DMF_CURATED_INVALID_COVERAGE: Invalid coverage limits
  - DMF_CURATED_CLAIMS_EXCEEDING_COVERAGE: Amount > coverage limit
  - DMF_CURATED_INVALID_DATE_SEQUENCE: Report before incident
  - DMF_CURATED_NEGATIVE_DAYS_TO_REPORT: Negative days calculation
  - DMF_CURATED_FRAUD_FLAG_RATE: Fraud percentage monitoring
  - DMF_CURATED_INVALID_COVERAGE_UTIL: Invalid utilization %
  - DMF_CURATED_HIGH_VALUE_CLAIMS: Claims > 100K DKK
  - DMF_CURATED_SAME_DAY_CLAIMS: Same-day reporting
  - DMF_CURATED_ORPHAN_CLAIMS: Referential integrity

DIM_POLICIES Table (12 DMFs):
-----------------------------
System DMFs:
  - ROW_COUNT: Monitor volume
  - FRESHNESS: Monitor staleness (updated_at)
  - NULL_COUNT: policy_id, policy_holder_name, coverage_limit, policy_type, risk_score
  - DUPLICATE_COUNT: policy_id
  - UNIQUE_COUNT: policy_type, risk_score, region

Custom DMFs:
  - DMF_CURATED_EXPIRED_POLICIES: Active policies with past end date

Navigate to the table's Data Quality page in Snowsight to review results!
******************************************************************************
*/

SELECT 'Curated data monitoring setup complete!' AS STATUS,
       'DIM_CLAIMS: 21 DMFs configured' AS CLAIMS_DMFS,
       'DIM_POLICIES: 12 DMFs configured' AS POLICIES_DMFS,
       'Schedule: 60 MINUTE' AS SCHEDULE,
       'Navigate to Data Quality tab in Snowsight to view results' AS NEXT_STEP;
