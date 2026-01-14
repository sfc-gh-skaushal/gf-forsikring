/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 05: Data Metric Functions (DMFs)
================================================================================
Purpose: Create and apply Data Metric Functions for automated data quality 
         monitoring - both system metrics and custom business rules
Author: Demo Setup Script
Date: 2025-01

VIGNETTE 2: Automating Data Trust with Quality & Lineage
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE GOVERNANCE_ADMIN;
USE WAREHOUSE INSURANCECO_ADMIN_WH;
USE DATABASE INSURANCECO;
USE SCHEMA GOVERNANCE;

-- ============================================================================
-- SECTION 2: SET DATA METRIC SCHEDULE (MUST BE DONE FIRST)
-- ============================================================================

/*
 * IMPORTANT: The DATA_METRIC_SCHEDULE must be set on the table BEFORE
 * adding any Data Metric Functions. This is required for DMF association.
 */

-- Set DMF schedule for the table FIRST (before adding any DMFs)
-- Using time-based schedule for the demo
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    SET DATA_METRIC_SCHEDULE = 'USING CRON 0 * * * * UTC';

-- Alternative schedules (uncomment one if preferred):
-- SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';  -- Runs when data changes
-- SET DATA_METRIC_SCHEDULE = '60 MINUTE';           -- Every 60 minutes

-- ============================================================================
-- SECTION 2B: SYSTEM DATA METRIC FUNCTIONS (OPTIONAL)
-- ============================================================================

/*
 * Snowflake provides built-in system DMFs for common quality checks:
 * - NULL_COUNT: Counts NULL values
 * - DUPLICATE_COUNT: Identifies duplicates
 * - UNIQUE_COUNT: Counts distinct values
 * - ROW_COUNT: Counts total rows
 * - FRESHNESS: Monitors data staleness (requires TIMESTAMP column)
 * 
 * NOTE: System DMFs may not be available in all Snowflake editions.
 * If you get "Function does not exist" errors, skip this section
 * and use the custom DMFs in Section 3 instead.
 * 
 * To use system DMFs, uncomment the statements below:
 */

-- NULL count monitoring on critical columns (uncomment if available)
-- ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
--     ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
--     ON (claim_id);

-- ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
--     ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT
--     ON (policy_id);

-- Duplicate detection on business key (uncomment if available)
-- ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
--     ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT
--     ON (claim_id);

-- ============================================================================
-- SECTION 3: CREATE CUSTOM DATA METRIC FUNCTIONS
-- ============================================================================

/*
 * Custom DMFs are SQL functions that return a single numeric value.
 * They are executed by Snowflake's scheduler and results are stored for 
 * trend analysis and alerting.
 */

-- DMF 1: Claims Exceeding Coverage Limit
-- Business Rule: Claim amount should not exceed policy coverage limit
CREATE OR REPLACE DATA METRIC FUNCTION DMF_CLAIMS_EXCEEDING_COVERAGE(
    ARG_T TABLE(
        claim_amount NUMBER,
        policy_coverage_limit NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts claims where claim_amount exceeds policy_coverage_limit - indicates potential fraud or data error'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount > policy_coverage_limit
$$;

-- DMF 2: High Fraud Flag Rate
-- Business Rule: Monitor the percentage of claims flagged for fraud
CREATE OR REPLACE DATA METRIC FUNCTION DMF_FRAUD_FLAG_RATE(
    ARG_T TABLE(
        fraud_flag BOOLEAN
    )
)
RETURNS NUMBER
COMMENT = 'Returns percentage of claims flagged for potential fraud - alert if above threshold'
AS
$$
    SELECT ROUND(
        (SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END)::FLOAT / 
         NULLIF(COUNT(*), 0)) * 100, 
        2
    )
    FROM ARG_T
$$;

-- DMF 3: Invalid Date Sequence
-- Business Rule: Date reported should not be before date of incident
CREATE OR REPLACE DATA METRIC FUNCTION DMF_INVALID_DATE_SEQUENCE(
    ARG_T TABLE(
        date_of_incident DATE,
        date_reported DATE
    )
)
RETURNS NUMBER
COMMENT = 'Counts records where date_reported is before date_of_incident - data quality issue'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE date_reported < date_of_incident
$$;

-- DMF 4: Missing Critical Fields
-- Business Rule: Core fields should not be NULL
CREATE OR REPLACE DATA METRIC FUNCTION DMF_MISSING_CRITICAL_FIELDS(
    ARG_T TABLE(
        claim_id VARCHAR,
        policy_id VARCHAR,
        claim_amount NUMBER,
        date_of_incident DATE,
        claim_type VARCHAR
    )
)
RETURNS NUMBER
COMMENT = 'Counts records missing any critical field - completeness check'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_id IS NULL
       OR policy_id IS NULL
       OR claim_amount IS NULL
       OR date_of_incident IS NULL
       OR claim_type IS NULL
$$;

-- DMF 5: Suspicious Same-Day Claims
-- Business Rule: Claims reported on incident date may need review
CREATE OR REPLACE DATA METRIC FUNCTION DMF_SAME_DAY_CLAIMS(
    ARG_T TABLE(
        date_of_incident DATE,
        date_reported DATE
    )
)
RETURNS NUMBER
COMMENT = 'Counts claims reported on the same day as incident - may indicate fraud pattern'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE date_of_incident = date_reported
$$;

-- DMF 6: High Value Claims Count
-- Business Rule: Monitor claims over 100,000 DKK threshold
CREATE OR REPLACE DATA METRIC FUNCTION DMF_HIGH_VALUE_CLAIMS(
    ARG_T TABLE(
        claim_amount NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts claims exceeding 100,000 DKK threshold - requires senior adjuster review'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE claim_amount > 100000
$$;

-- DMF 7: Invalid Email Format
-- Business Rule: Email addresses should match valid format
CREATE OR REPLACE DATA METRIC FUNCTION DMF_INVALID_EMAIL_FORMAT(
    ARG_T TABLE(
        policy_holder_email VARCHAR
    )
)
RETURNS NUMBER
COMMENT = 'Counts records with invalid email format - data quality issue'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_holder_email IS NOT NULL
      AND NOT REGEXP_LIKE(policy_holder_email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')
$$;

-- DMF 8: Coverage Utilization Warning
-- Business Rule: Alert when claims use more than 80% of coverage
CREATE OR REPLACE DATA METRIC FUNCTION DMF_HIGH_COVERAGE_UTILIZATION(
    ARG_T TABLE(
        claim_amount NUMBER,
        policy_coverage_limit NUMBER
    )
)
RETURNS NUMBER
COMMENT = 'Counts claims using more than 80% of coverage limit - risk indicator'
AS
$$
    SELECT COUNT(*)
    FROM ARG_T
    WHERE policy_coverage_limit > 0
      AND (claim_amount / policy_coverage_limit) > 0.8
$$;

-- ============================================================================
-- SECTION 4: APPLY CUSTOM DMFs TO DIM_CLAIMS
-- ============================================================================

-- Apply claims exceeding coverage DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_CLAIMS_EXCEEDING_COVERAGE
    ON (claim_amount, policy_coverage_limit);

-- Apply fraud flag rate DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_FRAUD_FLAG_RATE
    ON (fraud_flag);

-- Apply invalid date sequence DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_INVALID_DATE_SEQUENCE
    ON (date_of_incident, date_reported);

-- Apply missing critical fields DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_MISSING_CRITICAL_FIELDS
    ON (claim_id, policy_id, claim_amount, date_of_incident, claim_type);

-- Apply same-day claims DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_SAME_DAY_CLAIMS
    ON (date_of_incident, date_reported);

-- Apply high value claims DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_HIGH_VALUE_CLAIMS
    ON (claim_amount);

-- Apply invalid email DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_INVALID_EMAIL_FORMAT
    ON (policy_holder_email);

-- Apply high coverage utilization DMF
ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS
    ADD DATA METRIC FUNCTION INSURANCECO.GOVERNANCE.DMF_HIGH_COVERAGE_UTILIZATION
    ON (claim_amount, policy_coverage_limit);

-- ============================================================================
-- SECTION 5: VERIFY DMF SCHEDULING
-- ============================================================================

/*
 * DMF schedule was set in Section 2 (before adding DMFs).
 * Available schedule options:
 * - TRIGGER_ON_CHANGES: Runs when underlying data changes
 * - 'USING CRON <expr>': Cron-based scheduling
 * - '<N> MINUTE': Time interval (e.g., '60 MINUTE')
 * 
 * To change the schedule after DMFs are added:
 */

-- Verify current schedule
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE INSURANCECO.CURATED.DIM_CLAIMS;

-- To modify schedule (uncomment one):
-- ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
-- ALTER TABLE INSURANCECO.CURATED.DIM_CLAIMS SET DATA_METRIC_SCHEDULE = '60 MINUTE';

-- ============================================================================
-- SECTION 6: CREATE DMF RESULTS MONITORING VIEW
-- ============================================================================

-- View to monitor DMF results over time
CREATE OR REPLACE VIEW V_DATA_QUALITY_DASHBOARD AS
SELECT
    measurement_time,
    table_name,
    metric_name,
    value AS metric_value,
    CASE 
        WHEN metric_name = 'DMF_CLAIMS_EXCEEDING_COVERAGE' AND value > 0 THEN 'CRITICAL'
        WHEN metric_name = 'DMF_FRAUD_FLAG_RATE' AND value > 20 THEN 'WARNING'
        WHEN metric_name = 'DMF_INVALID_DATE_SEQUENCE' AND value > 0 THEN 'CRITICAL'
        WHEN metric_name = 'DMF_MISSING_CRITICAL_FIELDS' AND value > 0 THEN 'CRITICAL'
        WHEN metric_name = 'NULL_COUNT' AND value > 0 THEN 'WARNING'
        WHEN metric_name = 'DUPLICATE_COUNT' AND value > 0 THEN 'CRITICAL'
        ELSE 'OK'
    END AS severity,
    CASE
        WHEN metric_name = 'DMF_CLAIMS_EXCEEDING_COVERAGE' THEN 'Claims exceed policy coverage limit'
        WHEN metric_name = 'DMF_FRAUD_FLAG_RATE' THEN 'Percentage of claims flagged for fraud'
        WHEN metric_name = 'DMF_INVALID_DATE_SEQUENCE' THEN 'Report date before incident date'
        WHEN metric_name = 'DMF_MISSING_CRITICAL_FIELDS' THEN 'Records with missing required fields'
        WHEN metric_name = 'DMF_SAME_DAY_CLAIMS' THEN 'Claims reported same day as incident'
        WHEN metric_name = 'DMF_HIGH_VALUE_CLAIMS' THEN 'Claims over 100,000 DKK'
        WHEN metric_name = 'DMF_INVALID_EMAIL_FORMAT' THEN 'Invalid email addresses'
        WHEN metric_name = 'DMF_HIGH_COVERAGE_UTILIZATION' THEN 'Claims using >80% of coverage'
        WHEN metric_name = 'NULL_COUNT' THEN 'NULL values in column'
        WHEN metric_name = 'DUPLICATE_COUNT' THEN 'Duplicate values found'
        ELSE metric_name
    END AS description
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
ORDER BY measurement_time DESC;

-- ============================================================================
-- SECTION 7: MANUALLY EXECUTE DMFs (For Demo)
-- ============================================================================

-- Trigger immediate DMF execution for demo purposes
-- Note: In production, this happens automatically based on schedule

-- Execute all DMFs on DIM_CLAIMS manually
-- EXECUTE DATA METRIC FUNCTION ON TABLE INSURANCECO.CURATED.DIM_CLAIMS;

-- ============================================================================
-- SECTION 8: QUERY DMF RESULTS
-- ============================================================================

-- Check current DMF status on DIM_CLAIMS
-- This query shows the latest results from DMF execution
SELECT 
    table_name,
    metric_database,
    metric_schema,
    metric_name,
    value,
    measurement_time
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
  AND table_schema = 'CURATED'
  AND table_name = 'DIM_CLAIMS'
ORDER BY measurement_time DESC
LIMIT 20;

-- ============================================================================
-- SECTION 9: SAMPLE QUERIES TO DEMONSTRATE DMF VALUE
-- ============================================================================

-- Query 1: Find claims exceeding coverage (validates DMF logic)
SELECT 
    claim_id,
    claim_amount,
    policy_coverage_limit,
    claim_amount - policy_coverage_limit AS excess_amount,
    adjuster_notes
FROM INSURANCECO.CURATED.DIM_CLAIMS
WHERE claim_amount > policy_coverage_limit;

-- Query 2: Show fraud flag distribution
SELECT 
    fraud_flag,
    COUNT(*) AS claim_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM INSURANCECO.CURATED.DIM_CLAIMS
GROUP BY fraud_flag;

-- Query 3: Find same-day reported claims (potential fraud pattern)
SELECT 
    claim_id,
    date_of_incident,
    date_reported,
    claim_type,
    claim_amount,
    fraud_flag
FROM INSURANCECO.CURATED.DIM_CLAIMS
WHERE date_of_incident = date_reported
ORDER BY claim_amount DESC;

-- Query 4: High value claims requiring senior review
SELECT 
    claim_id,
    claim_amount,
    claim_type,
    claim_status,
    fraud_flag,
    vehicle_make,
    vehicle_model
FROM INSURANCECO.CURATED.DIM_CLAIMS
WHERE claim_amount > 100000
ORDER BY claim_amount DESC;

-- ============================================================================
-- SECTION 10: DEMO SCRIPT - DATA QUALITY MONITORING
-- ============================================================================

/*
DEMO WALKTHROUGH - Vignette 2: Data Quality Monitoring

1. SHOW THE PROBLEM (Before State)
   - Open RAW_CLAIMS table
   - Point out: "Raw data has no quality guarantees"
   - Show a query that finds claims exceeding coverage
   - Point out: "Today, finding these issues requires manual investigation"

2. INTRODUCE DATA METRIC FUNCTIONS
   - Navigate to DIM_CLAIMS in Snowsight
   - Click on "Data Quality" tab (if available in UI)
   - Or run: SELECT * FROM V_DATA_QUALITY_DASHBOARD
   - Point out: "DMFs automatically monitor data quality 24/7"

3. SHOW CUSTOM DMFS FOR NULL/COMPLETENESS
   - Show DMF_MISSING_CRITICAL_FIELDS results
   - Point out: "We check that all required fields are populated"
   - Point out: "Critical fields are monitored for completeness"

4. SHOW CUSTOM DMFs
   - Show DMF_CLAIMS_EXCEEDING_COVERAGE results
   - Point out: "This business rule says claim shouldn't exceed coverage"
   - Show DMF_FRAUD_FLAG_RATE
   - Point out: "We monitor fraud rate trends over time"

5. DEMONSTRATE ALERT SCENARIO
   - Run the query showing claims exceeding coverage
   - Point out: "DMF caught 5 claims that violate business rules"
   - Show how this would trigger an alert to the data steward

6. SHOW DMF RESULTS HISTORY
   - Query SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
   - Point out: "All results are stored for trend analysis"
   - Point out: "You can see quality improving or degrading over time"

KEY TALKING POINTS:
- "DMFs are just SQL - your team already knows how to write them"
- "Results are stored and trended automatically"
- "No separate tool to manage - it's built into Snowflake"
- "Catches issues before they reach business reports"
*/

SELECT 'Data Metric Functions setup complete!' AS STATUS,
       '8 custom DMFs created and applied' AS CUSTOM_DMFS,
       'System DMFs commented out (enable if available in your edition)' AS SYSTEM_DMFS,
       'Ready for quality monitoring demonstration' AS NEXT_STEP;
