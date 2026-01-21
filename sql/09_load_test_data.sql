/*
================================================================================
 GF FORSIKRING - LOAD 1000 TEST RECORDS WITH BAD DATA FOR DMF TESTING
================================================================================
 Purpose: Load test data with intentional bad records to trigger DMF failures
 
 Bad Data Included:
 ==================
 RAW_CLAIMS (1000 records):
   - 15 NULL claim_ids           -> Triggers NULL_COUNT DMF
   - 15 NULL policy_ids          -> Triggers NULL_COUNT DMF
   - 15 NULL amounts             -> Triggers NULL_COUNT DMF
   - 15 duplicate claim_ids      -> Triggers DUPLICATE_COUNT DMF
   - 15 negative amounts         -> Triggers custom negative amount DMF
   - 15 future incident dates    -> Triggers custom future date DMF
   - 15 zero amounts             -> Triggers amount validation
   - 15 huge amounts (50M+)      -> Triggers outlier detection
   - 15 NULL incident dates      -> Triggers NULL_COUNT DMF
   - 15 fraud-flagged records    -> Higher fraud rate for DMF_FRAUD_FLAG_RATE
   
 RAW_POLICIES (1000 records):
   - 15 NULL policy_ids          -> Triggers NULL_COUNT DMF
   - 15 NULL names               -> Triggers NULL_COUNT DMF
   - 15 NULL coverage limits     -> Triggers NULL_COUNT DMF
   - 15 duplicate policy_ids     -> Triggers DUPLICATE_COUNT DMF
   - 15 negative premiums        -> Triggers premium validation
   - 15 NULL emails              -> Triggers NULL_COUNT DMF
   - 15 NULL CPRs                -> Triggers NULL_COUNT DMF
   - 15 zero coverage limits     -> Triggers coverage validation
   - 15 invalid driver ages      -> Triggers age validation
   - 15 negative years licensed  -> Triggers license validation

================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE INSURANCECO_TRANSFORM_WH;
USE DATABASE INSURANCECO;

-- ============================================================================
-- SECTION 2: CREATE INTERNAL STAGE FOR FILE UPLOAD
-- ============================================================================

USE SCHEMA RAW;

-- Create a stage for uploading CSV files
CREATE OR REPLACE STAGE STG_TEST_DATA
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for test data CSV files';

-- ============================================================================
-- SECTION 3: CREATE FILE FORMAT FOR CSV
-- ============================================================================

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================================================
-- SECTION 4: UPLOAD FILES TO STAGE (Run these in Snowsight or SnowSQL)
-- ============================================================================

/*
 * OPTION A: Using Snowsight UI
 * ----------------------------
 * 1. Navigate to Data > Databases > INSURANCECO > RAW > Stages > STG_TEST_DATA
 * 2. Click "+ Files" button
 * 3. Upload raw_claims_1000.csv and raw_policies_1000.csv
 * 
 * OPTION B: Using SnowSQL CLI
 * ---------------------------
 * Run from your local terminal:
 * 
 * snowsql -a <account> -u <user> -d INSURANCECO -s RAW
 * PUT file:///path/to/data/raw_claims_1000.csv @STG_TEST_DATA;
 * PUT file:///path/to/data/raw_policies_1000.csv @STG_TEST_DATA;
 */

-- Verify files are staged (run after upload)
LIST @STG_TEST_DATA;

-- ============================================================================
-- SECTION 5: TRUNCATE EXISTING DATA (OPTIONAL - for clean test)
-- ============================================================================

-- Uncomment to clear existing data before loading
-- TRUNCATE TABLE RAW_CLAIMS;
-- TRUNCATE TABLE RAW_POLICIES;

-- ============================================================================
-- SECTION 6: LOAD RAW_CLAIMS DATA
-- ============================================================================

COPY INTO RAW_CLAIMS (
    claim_id,
    policy_id,
    claim_amount,
    policy_coverage_limit,
    date_of_incident,
    date_reported,
    claim_type,
    claim_status,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    damage_description,
    fraud_flag,
    adjuster_notes
)
FROM (
    SELECT 
        NULLIF($1, ''),           -- claim_id
        NULLIF($2, ''),           -- policy_id
        TRY_TO_NUMBER($3),        -- claim_amount
        TRY_TO_NUMBER($4),        -- policy_coverage_limit
        TRY_TO_DATE($5),          -- date_of_incident
        TRY_TO_DATE($6),          -- date_reported
        $7,                       -- claim_type
        $8,                       -- claim_status
        NULLIF($9, ''),           -- policy_holder_name
        NULLIF($10, ''),          -- policy_holder_email
        NULLIF($11, ''),          -- policy_holder_cpr
        $12,                      -- address
        $13,                      -- city
        $14,                      -- postal_code
        $15,                      -- vehicle_make
        $16,                      -- vehicle_model
        TRY_TO_NUMBER($17),       -- vehicle_year
        $18,                      -- damage_description
        TRY_TO_BOOLEAN($19),      -- fraud_flag
        $20                       -- adjuster_notes
    FROM @STG_TEST_DATA/raw_claims_1000.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Verify load
SELECT 'RAW_CLAIMS loaded' AS status, COUNT(*) AS record_count FROM RAW_CLAIMS;

-- ============================================================================
-- SECTION 7: LOAD RAW_POLICIES DATA
-- ============================================================================

COPY INTO RAW_POLICIES (
    policy_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    policy_type,
    coverage_limit,
    premium_annual,
    policy_start_date,
    policy_end_date,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_vin,
    driver_age,
    years_licensed,
    previous_claims_count,
    risk_score
)
FROM (
    SELECT 
        NULLIF($1, ''),           -- policy_id
        NULLIF($2, ''),           -- policy_holder_name
        NULLIF($3, ''),           -- policy_holder_email
        NULLIF($4, ''),           -- policy_holder_cpr
        $5,                       -- address
        $6,                       -- city
        $7,                       -- postal_code
        $8,                       -- policy_type
        TRY_TO_NUMBER($9),        -- coverage_limit
        TRY_TO_NUMBER($10),       -- premium_annual
        TRY_TO_DATE($11),         -- policy_start_date
        TRY_TO_DATE($12),         -- policy_end_date
        $13,                      -- vehicle_make
        $14,                      -- vehicle_model
        TRY_TO_NUMBER($15),       -- vehicle_year
        $16,                      -- vehicle_vin
        TRY_TO_NUMBER($17),       -- driver_age
        TRY_TO_NUMBER($18),       -- years_licensed
        TRY_TO_NUMBER($19),       -- previous_claims_count
        $20                       -- risk_score
    FROM @STG_TEST_DATA/raw_policies_1000.csv
)
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Verify load
SELECT 'RAW_POLICIES loaded' AS status, COUNT(*) AS record_count FROM RAW_POLICIES;

-- ============================================================================
-- SECTION 8: VERIFY BAD DATA WAS LOADED
-- ============================================================================

-- Check for NULL values in claims
SELECT 
    'RAW_CLAIMS NULL Analysis' AS check_type,
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) AS null_claim_ids,
    SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END) AS null_policy_ids,
    SUM(CASE WHEN claim_amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
    SUM(CASE WHEN date_of_incident IS NULL THEN 1 ELSE 0 END) AS null_incident_dates,
    SUM(CASE WHEN policy_holder_cpr IS NULL THEN 1 ELSE 0 END) AS null_cprs
FROM RAW_CLAIMS;

-- Check for bad amounts in claims
SELECT 
    'RAW_CLAIMS Amount Analysis' AS check_type,
    SUM(CASE WHEN claim_amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    SUM(CASE WHEN claim_amount = 0 THEN 1 ELSE 0 END) AS zero_amounts,
    SUM(CASE WHEN claim_amount > 10000000 THEN 1 ELSE 0 END) AS huge_amounts
FROM RAW_CLAIMS;

-- Check for future dates in claims
SELECT 
    'RAW_CLAIMS Date Analysis' AS check_type,
    SUM(CASE WHEN date_of_incident > CURRENT_DATE() THEN 1 ELSE 0 END) AS future_incident_dates
FROM RAW_CLAIMS;

-- Check for duplicates in claims
SELECT 
    'RAW_CLAIMS Duplicate Analysis' AS check_type,
    COUNT(*) - COUNT(DISTINCT claim_id) AS duplicate_claim_ids
FROM RAW_CLAIMS
WHERE claim_id IS NOT NULL;

-- Check for fraud flags
SELECT 
    'RAW_CLAIMS Fraud Analysis' AS check_type,
    SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) AS fraud_flagged,
    ROUND(SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_percentage
FROM RAW_CLAIMS;

-- Check for NULL values in policies
SELECT 
    'RAW_POLICIES NULL Analysis' AS check_type,
    SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END) AS null_policy_ids,
    SUM(CASE WHEN policy_holder_name IS NULL THEN 1 ELSE 0 END) AS null_names,
    SUM(CASE WHEN coverage_limit IS NULL THEN 1 ELSE 0 END) AS null_coverage,
    SUM(CASE WHEN policy_holder_email IS NULL THEN 1 ELSE 0 END) AS null_emails,
    SUM(CASE WHEN policy_holder_cpr IS NULL THEN 1 ELSE 0 END) AS null_cprs
FROM RAW_POLICIES;

-- Check for bad values in policies
SELECT 
    'RAW_POLICIES Value Analysis' AS check_type,
    SUM(CASE WHEN premium_annual < 0 THEN 1 ELSE 0 END) AS negative_premiums,
    SUM(CASE WHEN coverage_limit = 0 THEN 1 ELSE 0 END) AS zero_coverage,
    SUM(CASE WHEN driver_age < 0 OR driver_age > 120 THEN 1 ELSE 0 END) AS invalid_ages,
    SUM(CASE WHEN years_licensed < 0 THEN 1 ELSE 0 END) AS negative_years_licensed
FROM RAW_POLICIES;

-- ============================================================================
-- SECTION 9: POPULATE CURATED LAYER (DIM_CLAIMS)
-- ============================================================================

USE SCHEMA CURATED;

-- Truncate for clean load (optional)
-- TRUNCATE TABLE DIM_CLAIMS;

-- Insert transformed data into DIM_CLAIMS (matching actual table structure)
INSERT INTO DIM_CLAIMS (
    claim_id,
    policy_id,
    claim_amount,
    policy_coverage_limit,
    coverage_utilization_pct,
    date_of_incident,
    date_reported,
    days_to_report,
    claim_type,
    claim_status,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    region,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_age,
    damage_description,
    adjuster_notes,
    fraud_flag,
    exceeds_coverage,
    high_value_claim,
    created_at,
    updated_at,
    source_system,
    data_quality_score
)
SELECT 
    claim_id,
    policy_id,
    claim_amount,
    policy_coverage_limit,
    CASE WHEN policy_coverage_limit > 0 
         THEN ROUND((claim_amount / policy_coverage_limit) * 100, 2)
         ELSE 0 
    END AS coverage_utilization_pct,
    date_of_incident,
    date_reported,
    DATEDIFF('day', date_of_incident, date_reported) AS days_to_report,
    claim_type,
    claim_status,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    -- Calculate region from postal code
    CASE 
        WHEN LEFT(postal_code, 1) IN ('1', '2', '3') THEN 'Greater Copenhagen'
        WHEN LEFT(postal_code, 1) IN ('4', '5') THEN 'Zealand & Funen'
        WHEN LEFT(postal_code, 1) IN ('6', '7') THEN 'Central Jutland'
        WHEN LEFT(postal_code, 1) IN ('8', '9') THEN 'North Jutland'
        ELSE 'Unknown'
    END AS region,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    YEAR(CURRENT_DATE()) - vehicle_year AS vehicle_age,
    damage_description,
    adjuster_notes,
    fraud_flag,
    claim_amount > policy_coverage_limit AS exceeds_coverage,
    claim_amount > 100000 AS high_value_claim,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS updated_at,
    'RAW_CLAIMS' AS source_system,
    -- Calculate data quality score
    CASE 
        WHEN claim_id IS NOT NULL 
             AND policy_id IS NOT NULL 
             AND claim_amount IS NOT NULL 
             AND date_of_incident IS NOT NULL
        THEN 1.00
        ELSE 0.75
    END AS data_quality_score
FROM RAW.RAW_CLAIMS
WHERE claim_id IS NOT NULL 
  AND policy_id IS NOT NULL;  -- Filter out records with NULL claim_ids or policy_ids for curated

-- Verify curated claims
SELECT 'DIM_CLAIMS populated' AS status, COUNT(*) AS record_count FROM DIM_CLAIMS;

-- ============================================================================
-- SECTION 10: POPULATE CURATED LAYER (DIM_POLICIES)
-- ============================================================================

-- Truncate for clean load (optional)
-- TRUNCATE TABLE DIM_POLICIES;

-- Insert transformed data into DIM_POLICIES (matching actual table structure)
INSERT INTO DIM_POLICIES (
    policy_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    region,
    policy_type,
    coverage_limit,
    premium_annual,
    policy_start_date,
    policy_end_date,
    policy_term_months,
    is_active,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_vin,
    driver_age,
    years_licensed,
    previous_claims_count,
    risk_score,
    created_at,
    updated_at
)
SELECT 
    policy_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    -- Calculate region from postal code
    CASE 
        WHEN LEFT(postal_code, 1) IN ('1', '2', '3') THEN 'Greater Copenhagen'
        WHEN LEFT(postal_code, 1) IN ('4', '5') THEN 'Zealand & Funen'
        WHEN LEFT(postal_code, 1) IN ('6', '7') THEN 'Central Jutland'
        WHEN LEFT(postal_code, 1) IN ('8', '9') THEN 'North Jutland'
        ELSE 'Unknown'
    END AS region,
    policy_type,
    coverage_limit,
    premium_annual,
    policy_start_date,
    policy_end_date,
    -- Calculate policy term in months
    DATEDIFF('month', policy_start_date, policy_end_date) AS policy_term_months,
    -- Determine if policy is active
    CASE WHEN policy_end_date >= CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_active,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_vin,
    driver_age,
    years_licensed,
    previous_claims_count,
    risk_score,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS updated_at
FROM RAW.RAW_POLICIES
WHERE policy_id IS NOT NULL;  -- Filter out records with NULL policy_ids for curated

-- Verify curated policies
SELECT 'DIM_POLICIES populated' AS status, COUNT(*) AS record_count FROM DIM_POLICIES;

-- ============================================================================
-- SECTION 11: POPULATE ANALYTICS LAYER (AGG_CLAIMS_EXECUTIVE)
-- ============================================================================

USE SCHEMA ANALYTICS;

-- Recreate aggregated executive dashboard data
CREATE OR REPLACE TABLE AGG_CLAIMS_EXECUTIVE AS
SELECT
    DATE_TRUNC('WEEK', date_reported) AS report_week,
    region,
    claim_type,
    COUNT(*) AS total_claims,
    SUM(claim_amount) AS total_claim_value,
    AVG(claim_amount) AS avg_claim_value,
    MIN(claim_amount) AS min_claim_value,
    MAX(claim_amount) AS max_claim_value,
    SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) AS approved_claims,
    SUM(CASE WHEN claim_status = 'Rejected' THEN 1 ELSE 0 END) AS rejected_claims,
    SUM(CASE WHEN claim_status = 'Pending' THEN 1 ELSE 0 END) AS pending_claims,
    SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) AS fraud_flagged_claims,
    AVG(days_to_report) AS avg_days_to_report,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS refreshed_at
FROM CURATED.DIM_CLAIMS
GROUP BY 
    DATE_TRUNC('WEEK', date_reported),
    region,
    claim_type;

ALTER TABLE AGG_CLAIMS_EXECUTIVE SET COMMENT = 'Pre-aggregated executive dashboard data with weekly claims summary by region and type';

-- Set data metric schedule for freshness monitoring
ALTER TABLE AGG_CLAIMS_EXECUTIVE SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Verify analytics aggregates
SELECT 'AGG_CLAIMS_EXECUTIVE populated' AS status, COUNT(*) AS record_count FROM AGG_CLAIMS_EXECUTIVE;

-- ============================================================================
-- SECTION 12: POPULATE DATA SCIENCE LAYER (FRAUD_DETECTION_FEATURES)
-- ============================================================================

USE SCHEMA DATA_SCIENCE;

-- Recreate fraud detection feature table
CREATE OR REPLACE TABLE FRAUD_DETECTION_FEATURES AS
SELECT
    c.claim_id,
    -- Target variable
    c.fraud_flag AS is_fraud,
    -- Numeric features
    c.claim_amount,
    c.days_to_report,
    COALESCE(p.coverage_limit, 0) AS policy_coverage,
    COALESCE(p.premium_annual, 0) AS policy_premium,
    COALESCE(p.driver_age, 0) AS policyholder_age,
    COALESCE(p.policy_term_months, 0) AS policy_tenure_months,
    COALESCE(p.previous_claims_count, 0) AS previous_claims,
    COALESCE(p.years_licensed, 0) AS years_licensed,
    -- Derived features
    CASE WHEN p.coverage_limit > 0 
         THEN c.claim_amount / p.coverage_limit 
         ELSE 0 
    END AS claim_to_coverage_ratio,
    -- Categorical encodings
    CASE c.claim_type
        WHEN 'Auto' THEN 1
        WHEN 'Property' THEN 2
        WHEN 'Health' THEN 3
        WHEN 'Life' THEN 4
        WHEN 'Travel' THEN 5
        ELSE 0
    END AS claim_type_encoded,
    CASE c.region
        WHEN 'Greater Copenhagen' THEN 1
        WHEN 'Zealand & Funen' THEN 2
        WHEN 'Central Jutland' THEN 3
        WHEN 'North Jutland' THEN 4
        ELSE 0
    END AS region_encoded,
    CASE p.risk_score
        WHEN 'Low' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'High' THEN 3
        ELSE 0
    END AS risk_score_encoded,
    CASE p.policy_type
        WHEN 'Basic' THEN 1
        WHEN 'Standard' THEN 2
        WHEN 'Premium' THEN 3
        WHEN 'Enterprise' THEN 4
        ELSE 0
    END AS policy_type_encoded,
    -- Timestamp
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS feature_created_at
FROM CURATED.DIM_CLAIMS c
LEFT JOIN CURATED.DIM_POLICIES p ON c.policy_id = p.policy_id;

ALTER TABLE FRAUD_DETECTION_FEATURES SET COMMENT = 'ML-ready feature table for fraud detection model training';

-- Set data metric schedule
ALTER TABLE FRAUD_DETECTION_FEATURES SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Verify data science features
SELECT 'FRAUD_DETECTION_FEATURES populated' AS status, COUNT(*) AS record_count FROM FRAUD_DETECTION_FEATURES;

-- ============================================================================
-- SECTION 13: FINAL DATA QUALITY SUMMARY
-- ============================================================================

SELECT '=== DATA QUALITY SUMMARY ===' AS section;

-- Summary of all tables
SELECT 
    'RAW_CLAIMS' AS table_name, 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) AS null_primary_keys
FROM RAW.RAW_CLAIMS
UNION ALL
SELECT 
    'RAW_POLICIES', 
    COUNT(*),
    SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END)
FROM RAW.RAW_POLICIES
UNION ALL
SELECT 
    'DIM_CLAIMS', 
    COUNT(*),
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END)
FROM CURATED.DIM_CLAIMS
UNION ALL
SELECT 
    'DIM_POLICIES', 
    COUNT(*),
    SUM(CASE WHEN policy_id IS NULL THEN 1 ELSE 0 END)
FROM CURATED.DIM_POLICIES
UNION ALL
SELECT 
    'AGG_CLAIMS_EXECUTIVE', 
    COUNT(*),
    0
FROM ANALYTICS.AGG_CLAIMS_EXECUTIVE
UNION ALL
SELECT 
    'FRAUD_DETECTION_FEATURES', 
    COUNT(*),
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END)
FROM DATA_SCIENCE.FRAUD_DETECTION_FEATURES;

-- ============================================================================
-- SECTION 14: VIEW DMF RESULTS
-- ============================================================================

/*
 * Data Metric Functions will run automatically based on their schedule.
 * To see results, query the monitoring results view:
 */

SELECT 
    measurement_time,
    metric_database,
    metric_schema,
    metric_name,
    table_database,
    table_schema,
    table_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'INSURANCECO'
ORDER BY measurement_time DESC
LIMIT 50;

/*
================================================================================
 EXPECTED DMF FAILURES AFTER LOADING:
================================================================================

 RAW LAYER:
 ----------
 - NULL_COUNT on claim_id: ~15 nulls
 - NULL_COUNT on policy_id: ~30 nulls (15 in claims + 15 in policies)
 - NULL_COUNT on claim_amount: ~15 nulls
 - DUPLICATE_COUNT on claim_id: ~15 duplicates
 - DUPLICATE_COUNT on policy_id: ~15 duplicates
 - Custom DMFs for negative amounts, future dates, etc.

 CURATED LAYER:
 --------------
 - Data quality should be better (nulls filtered out)
 - Some derived field issues may surface from bad source data
 
 ANALYTICS LAYER:
 ----------------
 - Aggregations may show anomalies from bad underlying data
 
 DATA SCIENCE LAYER:
 -------------------
 - Feature quality checks
 - Higher fraud rate visible in fraud_flag column

================================================================================
*/

COMMIT;
