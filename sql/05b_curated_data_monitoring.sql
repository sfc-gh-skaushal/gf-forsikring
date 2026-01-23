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

-- Run as ACCOUNTADMIN for full privileges to set up DMFs
USE ROLE ACCOUNTADMIN;

-- Grant privileges on CURATED schema for DATA_ENGINEER to view data
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;

-- Grant privileges for viewing DMF results to other roles
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_ANALYST;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_SCIENTIST;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE DATA_STEWARD;

/*
******************************************************************************
* SECTION 2: SET DATA METRIC SCHEDULE ON CURATED TABLES
******************************************************************************
*/
use role data_engineer;
-- Continue as ACCOUNTADMIN (has full privileges to modify tables and add DMFs)
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;
USE SCHEMA CURATED;

-- Define schedule on DIM_CLAIMS (runs every 60 minutes)
ALTER TABLE DIM_CLAIMS SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Define schedule on DIM_POLICIES (runs every 60 minutes)
ALTER TABLE DIM_POLICIES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

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

-- NOTE: DMF_CURATED_EXPIRED_POLICIES was removed because DMFs cannot use 
-- non-deterministic functions like CURRENT_DATE(). 
-- To check for expired policies, use a regular SQL query instead:
-- SELECT COUNT(*) FROM DIM_POLICIES WHERE is_active = TRUE AND policy_end_date < CURRENT_DATE();

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

-- NOTE: No custom DMFs applied to DIM_POLICIES
-- DMF_CURATED_EXPIRED_POLICIES was removed (cannot use CURRENT_DATE in DMF)

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
    --measurement_time,
    metric_name,
    table_name,
   -- column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
  AND table_name = 'DIM_CLAIMS'
--ORDER BY measurement_time DESC
LIMIT 30;

-- View latest DMF results for DIM_POLICIES
SELECT 
   -- measurement_time,
    metric_name,
    table_name,
   -- column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
  AND table_name = 'DIM_POLICIES'
--ORDER BY measurement_time DESC
LIMIT 20;

-- Data Quality Dashboard: Show all issues in CURATED schema
SELECT 
    table_name,
    metric_name,
    MAX(value) AS latest_value,
    --MAX(measurement_time) AS last_checked,
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
  - (None - DMF_CURATED_EXPIRED_POLICIES removed due to CURRENT_DATE limitation)

Navigate to the table's Data Quality page in Snowsight to review results!
******************************************************************************
*/

SELECT 'Curated data monitoring setup complete!' AS STATUS,
       'DIM_CLAIMS: 21 DMFs configured' AS CLAIMS_DMFS,
       'DIM_POLICIES: 12 DMFs configured' AS POLICIES_DMFS,
       'Schedule: 60 MINUTE' AS SCHEDULE,
       'Navigate to Data Quality tab in Snowsight to view results' AS NEXT_STEP;

/*
******************************************************************************
* SECTION 11: ENABLE ANOMALY DETECTION FOR SUPPORTED DMFs
******************************************************************************
* NOTE: Anomaly detection is ONLY supported for ROW_COUNT and FRESHNESS DMFs.
*       Other system DMFs and custom DMFs do NOT support anomaly detection.
******************************************************************************
*/

USE SCHEMA INSURANCECO.CURATED;

-- ============================================================================
-- DIM_CLAIMS: Enable Anomaly Detection (only ROW_COUNT and FRESHNESS supported)
-- ============================================================================

ALTER TABLE DIM_CLAIMS
  MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()
  SET ANOMALY_DETECTION = TRUE;

ALTER TABLE DIM_CLAIMS
  MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (updated_at)
  SET ANOMALY_DETECTION = TRUE;

-- ============================================================================
-- DIM_POLICIES: Enable Anomaly Detection (only ROW_COUNT and FRESHNESS supported)
-- ============================================================================

ALTER TABLE DIM_POLICIES
  MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()
  SET ANOMALY_DETECTION = TRUE;

ALTER TABLE DIM_POLICIES
  MODIFY DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (updated_at)
  SET ANOMALY_DETECTION = TRUE;

-- ============================================================================
-- Verify Anomaly Detection is Enabled
-- ============================================================================

SELECT metric_name, ref_entity_name, anomaly_detection_status
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.CURATED.DIM_CLAIMS',
    REF_ENTITY_DOMAIN => 'TABLE'
))
UNION ALL
SELECT metric_name, ref_entity_name, anomaly_detection_status
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.CURATED.DIM_POLICIES',
    REF_ENTITY_DOMAIN => 'TABLE'
))
ORDER BY ref_entity_name, metric_name;

/*
******************************************************************************
* SECTION 12: SET UP DMF ALERTS USING SNOWFLAKE ALERTS
******************************************************************************
* Uses standard Snowflake ALERT objects to monitor DMF results and send
* email notifications when data quality issues are detected.
******************************************************************************
*/

USE ROLE ACCOUNTADMIN;
USE SCHEMA INSURANCECO.GOVERNANCE;

-- ============================================================================
-- Create Alerts for Critical CURATED Layer DMF Metrics
-- ============================================================================
-- NOTE: Assumes DMF_EMAIL_NOTIFICATION_INT already exists from 05a script
-- If not, create it first:
-- CREATE OR REPLACE NOTIFICATION INTEGRATION DMF_EMAIL_NOTIFICATION_INT
--   TYPE = EMAIL
--   ENABLED = TRUE
--   ALLOWED_RECIPIENTS = ('your-email@company.com');

USE ROLE DATA_ENGINEER;
USE WAREHOUSE INSURANCECO_ETL_WH;
USE SCHEMA INSURANCECO.GOVERNANCE;

-- Alert 1: Duplicate claim_ids in DIM_CLAIMS (CRITICAL - should never happen)
CREATE OR REPLACE ALERT ALERT_DIM_CLAIMS_DUPLICATE_ID
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_CLAIMS'
      AND metric_name = 'DUPLICATE_COUNT'
      AND value > 0
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'CRITICAL: Duplicate claim_ids in DIM_CLAIMS',
      'Alert: Duplicate claim_id values detected in curated DIM_CLAIMS table. This indicates a serious data integrity issue. Please investigate immediately.'
    );

ALTER ALERT ALERT_DIM_CLAIMS_DUPLICATE_ID RESUME;
ALTER ALERT ALERT_DIM_CLAIMS_DUPLICATE_ID SUSPEND;

-- Alert 2: Invalid claim amounts in curated layer
CREATE OR REPLACE ALERT ALERT_DIM_CLAIMS_INVALID_AMOUNT
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_CURATED_INVALID_CLAIM_AMOUNT'
      AND value > 0
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'CRITICAL: Invalid claim amounts in DIM_CLAIMS',
      'Alert: Claims with negative or zero amounts found in curated DIM_CLAIMS. Data validation in ETL pipeline may have failed.'
    );

ALTER ALERT ALERT_DIM_CLAIMS_INVALID_AMOUNT RESUME;
ALTER ALERT ALERT_DIM_CLAIMS_INVALID_AMOUNT SUSPEND;

-- Alert 3: Orphan claims (referential integrity violation)
CREATE OR REPLACE ALERT ALERT_DIM_CLAIMS_ORPHAN
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_CURATED_ORPHAN_CLAIMS'
      AND value > 0
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'CRITICAL: Orphan claims in DIM_CLAIMS',
      'Alert: Claims found with policy_id not in DIM_POLICIES. Referential integrity violated in curated layer.'
    );

ALTER ALERT ALERT_DIM_CLAIMS_ORPHAN RESUME;
ALTER ALERT ALERT_DIM_CLAIMS_ORPHAN SUSPEND;

-- Alert 4: Invalid date sequence
CREATE OR REPLACE ALERT ALERT_DIM_CLAIMS_INVALID_DATES
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_CURATED_INVALID_DATE_SEQUENCE'
      AND value > 0
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'WARNING: Invalid date sequence in DIM_CLAIMS',
      'Alert: Claims with report date before incident date found in curated layer. Please review data pipeline.'
    );

ALTER ALERT ALERT_DIM_CLAIMS_INVALID_DATES RESUME;
ALTER ALERT ALERT_DIM_CLAIMS_INVALID_DATES SUSPEND;

-- Alert 5: High fraud rate warning
CREATE OR REPLACE ALERT ALERT_DIM_CLAIMS_HIGH_FRAUD_RATE
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_CURATED_FRAUD_FLAG_RATE'
      AND value > 25
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'WARNING: High fraud rate detected in DIM_CLAIMS',
      'Alert: Fraud flag rate exceeds 25% threshold. This may indicate a data issue or require fraud investigation team review.'
    );

ALTER ALERT ALERT_DIM_CLAIMS_HIGH_FRAUD_RATE RESUME;
ALTER ALERT ALERT_DIM_CLAIMS_HIGH_FRAUD_RATE SUSPEND;

-- Alert 6: Duplicate policy_ids in DIM_POLICIES
CREATE OR REPLACE ALERT ALERT_DIM_POLICIES_DUPLICATE_ID
  WAREHOUSE = INSURANCECO_ETL_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'INSURANCECO'
      AND table_schema = 'CURATED'
      AND table_name = 'DIM_POLICIES'
      AND metric_name = 'DUPLICATE_COUNT'
      AND value > 0
      AND measurement_time > DATEADD('minute', -65, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DMF_EMAIL_NOTIFICATION_INT',
      'siddharth.kaushal@snowflake.com',
      'CRITICAL: Duplicate policy_ids in DIM_POLICIES',
      'Alert: Duplicate policy_id values detected in curated DIM_POLICIES table. This is a critical data integrity issue.'
    );

ALTER ALERT ALERT_DIM_POLICIES_DUPLICATE_ID RESUME;
ALTER ALERT ALERT_DIM_POLICIES_DUPLICATE_ID SUSPEND;

-- ============================================================================
-- Verify Alerts are Created and Running
-- ============================================================================

SHOW ALERTS IN SCHEMA INSURANCECO.GOVERNANCE;

/*
******************************************************************************
* CURATED LAYER ALERTS SUMMARY
******************************************************************************

6 Snowflake Alerts created for CURATED layer:

DIM_CLAIMS Alerts (5):
----------------------
1. ALERT_DIM_CLAIMS_DUPLICATE_ID     - Duplicate claim_ids (CRITICAL)
2. ALERT_DIM_CLAIMS_INVALID_AMOUNT   - Invalid claim amounts (CRITICAL)
3. ALERT_DIM_CLAIMS_ORPHAN           - Orphan claims (CRITICAL)
4. ALERT_DIM_CLAIMS_INVALID_DATES    - Invalid date sequence (WARNING)
5. ALERT_DIM_CLAIMS_HIGH_FRAUD_RATE  - Fraud rate > 25% (WARNING)

DIM_POLICIES Alerts (1):
------------------------
1. ALERT_DIM_POLICIES_DUPLICATE_ID   - Duplicate policy_ids (CRITICAL)

Anomaly Detection Enabled:
--------------------------
- DIM_CLAIMS: ROW_COUNT, FRESHNESS
- DIM_POLICIES: ROW_COUNT, FRESHNESS

******************************************************************************
*/
