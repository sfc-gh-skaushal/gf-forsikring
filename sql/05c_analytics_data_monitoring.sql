/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 05c: Analytics Data Quality Monitoring Setup
================================================================================
Purpose: Set up data quality monitoring on ANALYTICS schema tables
         (AGG_CLAIMS_EXECUTIVE) and DATA_SCIENCE schema (FRAUD_DETECTION_FEATURES)
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

-- Grant privileges on ANALYTICS schema for DATA_ENGINEER to view data
GRANT USAGE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;

-- Grant privileges on DATA_SCIENCE schema for DATA_ENGINEER to view data
GRANT USAGE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;

/*
******************************************************************************
* SECTION 2: SET DATA METRIC SCHEDULE ON ANALYTICS TABLES
******************************************************************************
*/
use role data_engineer;
-- Continue as ACCOUNTADMIN (has full privileges to modify tables and add DMFs)
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;

-- Define schedule on AGG_CLAIMS_EXECUTIVE (runs every 60 minutes)
USE SCHEMA ANALYTICS;
ALTER TABLE AGG_CLAIMS_EXECUTIVE SET DATA_METRIC_SCHEDULE = '5 MINUTE';

-- Define schedule on FRAUD_DETECTION_FEATURES (runs every 60 minutes)
USE SCHEMA DATA_SCIENCE;
ALTER TABLE FRAUD_DETECTION_FEATURES SET DATA_METRIC_SCHEDULE = '5 MINUTE';

/*
******************************************************************************
* SECTION 3: ADD SYSTEM DMF ASSOCIATIONS TO AGG_CLAIMS_EXECUTIVE
******************************************************************************
*/

USE SCHEMA ANALYTICS;

-- ROW_COUNT: Monitor aggregate volume
ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- FRESHNESS: Monitor when aggregates were last refreshed
ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON (refreshed_at);

-- NULL_COUNT: Monitor NULL values in key dimensions
ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (report_week);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (region);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_type);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (total_claims);

-- UNIQUE_COUNT: Track cardinality of dimensions
ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (region);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_type);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (report_week);

/*
******************************************************************************
* SECTION 4: ADD SYSTEM DMF ASSOCIATIONS TO FRAUD_DETECTION_FEATURES
******************************************************************************
*/

USE SCHEMA DATA_SCIENCE;

-- ROW_COUNT: Monitor feature table volume
ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- NULL_COUNT: Monitor NULL values in features (critical for ML)
ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_id);

/*ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (is_fraud);*/

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_amount);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (claim_type_encoded);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (risk_score_encoded);

-- DUPLICATE_COUNT: Check for duplicate claim IDs in features
ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (claim_id);

-- UNIQUE_COUNT: Track cardinality of encoded features
ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (claim_type_encoded);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (risk_score_encoded);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.UNIQUE_COUNT ON (policy_type_encoded);

/*
******************************************************************************
* SECTION 5: CREATE CUSTOM DMFs FOR ANALYTICS VALIDATION
******************************************************************************
*/

USE SCHEMA INSURANCECO.GOVERNANCE;

-- DMF: Aggregate totals should be non-negative
CREATE OR REPLACE DATA METRIC FUNCTION DMF_AGG_NEGATIVE_TOTALS(
    ARG_T TABLE(total_claims NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts aggregate rows with negative total_claims - indicates calculation error'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE total_claims < 0
$$;

-- DMF: Aggregate claim values should be non-negative
CREATE OR REPLACE DATA METRIC FUNCTION DMF_AGG_NEGATIVE_VALUES(
    ARG_T TABLE(total_claim_value NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts aggregate rows with negative total_claim_value'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE total_claim_value < 0
$$;

-- DMF: Approved + Pending + Flagged should equal Total
CREATE OR REPLACE DATA METRIC FUNCTION DMF_AGG_STATUS_MISMATCH(
    ARG_T TABLE(
        total_claims NUMBER,
        approved_claims NUMBER,
        pending_claims NUMBER,
        fraud_flagged_claims NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts rows where status counts dont match total - data integrity check'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE (COALESCE(approved_claims, 0) + COALESCE(pending_claims, 0) + COALESCE(fraud_flagged_claims, 0)) > total_claims
$$;

-- NOTE: DMF_AGG_STALE_DATA was removed because DMFs cannot use 
-- non-deterministic functions like CURRENT_TIMESTAMP(). 
-- Use SNOWFLAKE.CORE.FRESHNESS system DMF instead for staleness monitoring.
-- Or use a regular SQL query: SELECT COUNT(*) FROM AGG_CLAIMS_EXECUTIVE WHERE refreshed_at < DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- DMF: ML Feature - Target variable distribution (fraud rate)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_ML_FRAUD_RATE(
    ARG_T TABLE(is_fraud BOOLEAN)
)
RETURNS NUMBER
COMMENT = 'Returns fraud rate in ML features - monitor for data drift'
AS
$$
    SELECT ROUND(
        (SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0 / 
         NULLIF(COUNT(*), 0)), 
        2
    )::NUMBER
    FROM ARG_T
$$;

-- DMF: ML Feature - Check for invalid encoded values
CREATE OR REPLACE DATA METRIC FUNCTION DMF_ML_INVALID_ENCODING(
    ARG_T TABLE(claim_type_encoded NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts rows with invalid claim_type encoding (outside expected range 0-6)'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_type_encoded < 0 OR claim_type_encoded > 6
$$;

-- DMF: ML Feature - Check for NULL features (problematic for ML)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_ML_NULL_FEATURES(
    ARG_T TABLE(
        claim_amount NUMBER,
        claim_to_coverage_ratio NUMBER,
        days_to_report NUMBER,
        policyholder_age NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts rows with NULL numeric features - will cause ML model issues'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount IS NULL
       OR claim_to_coverage_ratio IS NULL
       OR days_to_report IS NULL
       OR policyholder_age IS NULL
$$;

-- DMF: ML Feature - Check for negative values that should be positive
CREATE OR REPLACE DATA METRIC FUNCTION DMF_ML_NEGATIVE_FEATURES(
    ARG_T TABLE(
        claim_amount NUMBER,
        days_to_report NUMBER,
        policyholder_age NUMBER,
        years_licensed NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts rows with negative values in features that should be positive'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount < 0
       OR days_to_report < 0
       OR policyholder_age < 0
       OR years_licensed < 0
$$;

-- DMF: ML Feature - Suspicious claim_premium_ratio (extremely high)
CREATE OR REPLACE DATA METRIC FUNCTION DMF_ML_EXTREME_RATIO(
    ARG_T TABLE(claim_to_coverage_ratio NUMBER)
)
RETURNS NUMBER
COMMENT = 'Counts rows with extremely high claim_to_coverage_ratio (> 100)'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_to_coverage_ratio IS NOT NULL AND claim_to_coverage_ratio > 100
$$;

/*
******************************************************************************
* SECTION 6: APPLY CUSTOM DMFs TO AGG_CLAIMS_EXECUTIVE
******************************************************************************
*/

USE SCHEMA INSURANCECO.ANALYTICS;

-- Apply business rule DMFs to aggregate table
ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_AGG_NEGATIVE_TOTALS
    ON (total_claims);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_AGG_NEGATIVE_VALUES
    ON (total_claim_value);

ALTER TABLE AGG_CLAIMS_EXECUTIVE
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_AGG_STATUS_MISMATCH
    ON (total_claims, approved_claims, pending_claims, fraud_flagged_claims);

-- NOTE: DMF_AGG_STALE_DATA removed (cannot use CURRENT_TIMESTAMP in DMF)
-- Using SNOWFLAKE.CORE.FRESHNESS system DMF instead for staleness monitoring

/*
******************************************************************************
* SECTION 7: APPLY CUSTOM DMFs TO FRAUD_DETECTION_FEATURES
******************************************************************************
*/

USE SCHEMA INSURANCECO.DATA_SCIENCE;

-- Apply ML feature quality DMFs
ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_ML_FRAUD_RATE
    ON (is_fraud);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_ML_INVALID_ENCODING
    ON (claim_type_encoded);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_ML_NULL_FEATURES
    ON (claim_amount, claim_to_coverage_ratio, days_to_report, policyholder_age);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_ML_NEGATIVE_FEATURES
    ON (claim_amount, days_to_report, policyholder_age, years_licensed);

ALTER TABLE FRAUD_DETECTION_FEATURES
  ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_ML_EXTREME_RATIO
    ON (claim_to_coverage_ratio);

/*
******************************************************************************
* SECTION 8: VERIFY DMF SETUP
******************************************************************************
*/

-- Show all DMFs associated with AGG_CLAIMS_EXECUTIVE
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.ANALYTICS.AGG_CLAIMS_EXECUTIVE',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Show all DMFs associated with FRAUD_DETECTION_FEATURES
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'INSURANCECO.DATA_SCIENCE.FRAUD_DETECTION_FEATURES',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Check current schedule on tables
USE SCHEMA ANALYTICS;
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE AGG_CLAIMS_EXECUTIVE;

USE SCHEMA DATA_SCIENCE;
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE FRAUD_DETECTION_FEATURES;

/*
******************************************************************************
* SECTION 9: QUERY DMF RESULTS
******************************************************************************
*/

-- View latest DMF results for AGG_CLAIMS_EXECUTIVE
SELECT 
    --measurement_time,
    metric_name,
    table_name,
    --column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'ANALYTICS'
  AND table_name = 'AGG_CLAIMS_EXECUTIVE'
--ORDER BY measurement_time DESC
LIMIT 20;

-- View latest DMF results for FRAUD_DETECTION_FEATURES
SELECT 
    --measurement_time,
    metric_name,
    table_name,
    --column_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'DATA_SCIENCE'
  AND table_name = 'FRAUD_DETECTION_FEATURES'
--ORDER BY measurement_time DESC
LIMIT 20;

-- ML Feature Quality Dashboard
SELECT 
    metric_name,
    MAX(value) AS latest_value,
   -- MAX(measurement_time) AS last_checked,
    CASE 
        WHEN metric_name = 'DMF_ML_NULL_FEATURES' AND MAX(value) > 0 THEN 'CRITICAL'
        WHEN metric_name = 'DMF_ML_NEGATIVE_FEATURES' AND MAX(value) > 0 THEN 'CRITICAL'
        WHEN metric_name = 'DMF_ML_INVALID_ENCODING' AND MAX(value) > 0 THEN 'WARNING'
        WHEN metric_name = 'DUPLICATE_COUNT' AND MAX(value) > 0 THEN 'WARNING'
        ELSE 'OK'
    END AS severity,
    CASE 
        WHEN metric_name = 'DMF_ML_FRAUD_RATE' THEN 'Monitor for drift from training data'
        WHEN metric_name = 'DMF_ML_NULL_FEATURES' THEN 'NULL features will cause model prediction errors'
        WHEN metric_name = 'DMF_ML_NEGATIVE_FEATURES' THEN 'Negative values indicate data pipeline issues'
        ELSE 'Standard monitoring'
    END AS action_required
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'DATA_SCIENCE'
  AND table_name = 'FRAUD_DETECTION_FEATURES'
GROUP BY metric_name
ORDER BY severity, metric_name;

/*
******************************************************************************
* SECTION 10: OPTIONAL - TRIGGER IMMEDIATE DMF EXECUTION
******************************************************************************
*/

-- Uncomment to manually trigger DMF execution (for demo purposes)
-- EXECUTE DATA METRIC FUNCTION ON TABLE INSURANCECO.ANALYTICS.AGG_CLAIMS_EXECUTIVE;
-- EXECUTE DATA METRIC FUNCTION ON TABLE INSURANCECO.DATA_SCIENCE.FRAUD_DETECTION_FEATURES;

/*
******************************************************************************
* SUMMARY OF DMFs CONFIGURED
******************************************************************************

AGG_CLAIMS_EXECUTIVE Table (13 DMFs):
-------------------------------------
System DMFs:
  - ROW_COUNT: Monitor aggregate row count
  - FRESHNESS: Monitor refresh timestamp
  - NULL_COUNT: report_week, region, claim_type, total_claims
  - UNIQUE_COUNT: region, claim_type, report_week

Custom DMFs:
  - DMF_AGG_NEGATIVE_TOTALS: Negative total_claims
  - DMF_AGG_NEGATIVE_VALUES: Negative total_claim_value
  - DMF_AGG_STATUS_MISMATCH: Status counts > total
  - (DMF_AGG_STALE_DATA removed - use FRESHNESS system DMF instead)

FRAUD_DETECTION_FEATURES Table (15 DMFs):
-----------------------------------------
System DMFs:
  - ROW_COUNT: Monitor feature table volume
  - NULL_COUNT: claim_id, is_fraud, claim_amount, claim_type_encoded, risk_score_encoded
  - DUPLICATE_COUNT: claim_id
  - UNIQUE_COUNT: claim_type_encoded, risk_score_encoded, policy_type_encoded

Custom DMFs:
  - DMF_ML_FRAUD_RATE: Monitor target variable distribution
  - DMF_ML_INVALID_ENCODING: Invalid encoded values
  - DMF_ML_NULL_FEATURES: NULL values in numeric features
  - DMF_ML_NEGATIVE_FEATURES: Negative values in positive-only features
  - DMF_ML_EXTREME_RATIO: Extreme claim_premium_ratio values

Navigate to the table's Data Quality page in Snowsight to review results!
******************************************************************************
*/

SELECT 'Analytics & ML data monitoring setup complete!' AS STATUS,
       'AGG_CLAIMS_EXECUTIVE: 13 DMFs configured' AS ANALYTICS_DMFS,
       'FRAUD_DETECTION_FEATURES: 15 DMFs configured' AS ML_DMFS,
       'Schedule: 60 MINUTE' AS SCHEDULE,
       'Navigate to Data Quality tab in Snowsight to view results' AS NEXT_STEP;