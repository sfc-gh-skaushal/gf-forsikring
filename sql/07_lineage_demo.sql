/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 07: Column-Level Lineage Demonstration
================================================================================
Purpose: Create views and transformations that demonstrate automatic 
         column-level lineage tracking in Snowflake
Author: Demo Setup Script
Date: 2025-01

VIGNETTE 2 (Continued): Understanding Data Flow with Lineage
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE DATA_ENGINEER;
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- SECTION 2: CREATE ANALYTICS VIEWS (For Lineage Demonstration)
-- ============================================================================

/*
 * These views create multiple levels of transformation from curated data.
 * Snowflake automatically tracks column-level lineage through these views.
 * In the Horizon UI, you can visualize the complete data flow.
 */

-- Analytics View 1: Claims Summary by Region
-- Shows aggregated claims data by geographic region
CREATE OR REPLACE VIEW V_CLAIMS_BY_REGION
    COMMENT = 'Regional claims analysis - aggregated by region and claim type. Source: DIM_CLAIMS'
AS
SELECT
    region,
    claim_type,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount,
    AVG(claim_amount) AS avg_claim_amount,
    MAX(claim_amount) AS max_claim_amount,
    SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS fraud_flagged_count,
    ROUND(SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct
FROM INSURANCECO.CURATED.DIM_CLAIMS
GROUP BY region, claim_type;

-- Analytics View 2: Monthly Claims Trend
-- Time-series view for trending analysis
CREATE OR REPLACE VIEW V_CLAIMS_MONTHLY_TREND
    COMMENT = 'Monthly claims trend analysis. Source: DIM_CLAIMS'
AS
SELECT
    DATE_TRUNC('MONTH', date_reported) AS report_month,
    claim_type,
    claim_status,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_amount,
    AVG(days_to_report) AS avg_days_to_report,
    SUM(CASE WHEN exceeds_coverage THEN 1 ELSE 0 END) AS coverage_exceeded_count
FROM INSURANCECO.CURATED.DIM_CLAIMS
GROUP BY 1, 2, 3;

-- Analytics View 3: High Risk Claims Dashboard
-- Filtered view for claims requiring attention
CREATE OR REPLACE VIEW V_HIGH_RISK_CLAIMS
    COMMENT = 'High risk claims requiring review. Combines fraud, coverage, and value indicators. Source: DIM_CLAIMS'
AS
SELECT
    c.claim_id,
    c.policy_id,
    c.claim_amount,
    c.policy_coverage_limit,
    c.coverage_utilization_pct,
    c.claim_type,
    c.claim_status,
    c.fraud_flag,
    c.exceeds_coverage,
    c.high_value_claim,
    c.days_to_report,
    c.region,
    c.vehicle_make,
    c.vehicle_model,
    c.vehicle_age,
    c.adjuster_notes,
    c.date_reported,
    -- Risk score calculation
    CASE 
        WHEN c.fraud_flag AND c.exceeds_coverage THEN 'CRITICAL'
        WHEN c.fraud_flag OR c.exceeds_coverage THEN 'HIGH'
        WHEN c.high_value_claim OR c.days_to_report = 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM INSURANCECO.CURATED.DIM_CLAIMS c
WHERE c.fraud_flag = TRUE 
   OR c.exceeds_coverage = TRUE 
   OR c.high_value_claim = TRUE
   OR c.days_to_report = 0;

-- Analytics View 4: Claims with Policy Details (Join)
-- Demonstrates lineage across joined tables
CREATE OR REPLACE VIEW V_CLAIMS_WITH_POLICY
    COMMENT = 'Claims enriched with policy details. Demonstrates cross-table lineage. Source: DIM_CLAIMS + DIM_POLICIES'
AS
SELECT
    c.claim_id,
    c.claim_amount,
    c.claim_type,
    c.claim_status,
    c.date_of_incident,
    c.date_reported,
    c.days_to_report,
    c.fraud_flag,
    c.region AS claim_region,
    -- Policy details
    p.policy_id,
    p.policy_type,
    p.coverage_limit AS policy_coverage,
    p.premium_annual,
    p.risk_score AS policy_risk_score,
    p.previous_claims_count,
    p.driver_age,
    p.years_licensed,
    -- Calculated fields
    ROUND(c.claim_amount / NULLIF(p.premium_annual, 0), 2) AS claim_to_premium_ratio,
    CASE 
        WHEN p.previous_claims_count > 2 THEN 'Frequent Claimant'
        WHEN p.risk_score = 'HIGH' THEN 'High Risk Policy'
        ELSE 'Standard'
    END AS customer_segment
FROM INSURANCECO.CURATED.DIM_CLAIMS c
JOIN INSURANCECO.CURATED.DIM_POLICIES p ON c.policy_id = p.policy_id;

-- ============================================================================
-- SECTION 3: CREATE AGGREGATE TABLES (For BI Dashboard)
-- ============================================================================

-- Executive Dashboard Table
-- Pre-aggregated for fast dashboard queries
CREATE OR REPLACE TABLE AGG_CLAIMS_EXECUTIVE AS
SELECT
    DATE_TRUNC('WEEK', date_reported) AS report_week,
    region,
    claim_type,
    COUNT(*) AS total_claims,
    COUNT(CASE WHEN claim_status = 'approved' THEN 1 END) AS approved_claims,
    COUNT(CASE WHEN claim_status = 'pending' THEN 1 END) AS pending_claims,
    COUNT(CASE WHEN claim_status = 'flagged' THEN 1 END) AS flagged_claims,
    SUM(claim_amount) AS total_claim_value,
    AVG(claim_amount) AS avg_claim_value,
    SUM(CASE WHEN fraud_flag THEN claim_amount ELSE 0 END) AS fraud_flagged_value,
    AVG(days_to_report) AS avg_reporting_delay,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS refreshed_at
FROM INSURANCECO.CURATED.DIM_CLAIMS
GROUP BY 1, 2, 3;

-- Add comment to table
ALTER TABLE AGG_CLAIMS_EXECUTIVE SET COMMENT = 'Pre-aggregated executive dashboard data. Refreshed by scheduled task. Source: DIM_CLAIMS';

-- ============================================================================
-- SECTION 4: CREATE ML FEATURE TABLE (For Data Science)
-- ============================================================================

USE SCHEMA INSURANCECO.DATA_SCIENCE;

-- Feature table for fraud detection model
CREATE OR REPLACE TABLE FRAUD_DETECTION_FEATURES AS
SELECT
    c.claim_id,
    -- Target variable
    c.fraud_flag AS is_fraud,
    -- Claim features
    c.claim_amount,
    c.coverage_utilization_pct,
    c.days_to_report,
    CASE c.claim_type 
        WHEN 'collision' THEN 1
        WHEN 'theft' THEN 2
        WHEN 'vandalism' THEN 3
        WHEN 'weather' THEN 4
        WHEN 'glass' THEN 5
        WHEN 'fire' THEN 6
        ELSE 0 
    END AS claim_type_encoded,
    c.exceeds_coverage::INT AS exceeds_coverage,
    c.high_value_claim::INT AS high_value,
    -- Vehicle features
    c.vehicle_age,
    CASE c.vehicle_make
        WHEN 'BMW' THEN 1
        WHEN 'Mercedes' THEN 2
        WHEN 'Audi' THEN 3
        WHEN 'Porsche' THEN 4
        WHEN 'Tesla' THEN 5
        WHEN 'Land Rover' THEN 6
        ELSE 0
    END AS luxury_brand,
    -- Policy features from join
    p.previous_claims_count,
    p.driver_age,
    p.years_licensed,
    CASE p.risk_score
        WHEN 'HIGH' THEN 3
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 1
        ELSE 0
    END AS risk_score_encoded,
    CASE p.policy_type
        WHEN 'basic' THEN 1
        WHEN 'comprehensive' THEN 2
        WHEN 'premium' THEN 3
        ELSE 0
    END AS policy_type_encoded,
    -- Time features
    DAYOFWEEK(c.date_of_incident) AS incident_day_of_week,
    MONTH(c.date_of_incident) AS incident_month,
    -- Derived features
    p.driver_age - p.years_licensed AS age_when_licensed,
    c.claim_amount / NULLIF(p.premium_annual, 0) AS claim_premium_ratio
FROM INSURANCECO.CURATED.DIM_CLAIMS c
JOIN INSURANCECO.CURATED.DIM_POLICIES p ON c.policy_id = p.policy_id;

-- Add comment to table
ALTER TABLE FRAUD_DETECTION_FEATURES SET COMMENT = 'ML features for fraud detection model. Derived from DIM_CLAIMS and DIM_POLICIES.';

-- ============================================================================
-- SECTION 5: QUERY LINEAGE METADATA
-- ============================================================================

/*
 * Snowflake stores lineage information in ACCOUNT_USAGE views.
 * These queries help you understand data flow programmatically.
 */

-- Query to show object-level lineage
-- Shows which objects depend on DIM_CLAIMS
-- SELECT 
--     referencing_object_name,
--     referencing_object_domain,
--     referenced_object_name,
--     referenced_object_domain
-- FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
-- WHERE referenced_object_name = 'DIM_CLAIMS'
--   AND referenced_database_name = 'INSURANCECO';

-- Query to show access history (who queried what)
-- SELECT 
--     query_start_time,
--     user_name,
--     role_name,
--     direct_objects_accessed,
--     base_objects_accessed
-- FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
-- WHERE ARRAY_CONTAINS('INSURANCECO.CURATED.DIM_CLAIMS'::VARIANT, base_objects_accessed)
-- ORDER BY query_start_time DESC
-- LIMIT 20;

-- ============================================================================
-- SECTION 6: LINEAGE VISUALIZATION HELPERS
-- ============================================================================

USE SCHEMA INSURANCECO.GOVERNANCE;

-- View to display lineage summary for documentation
CREATE OR REPLACE VIEW V_LINEAGE_SUMMARY AS
SELECT 
    'DIM_CLAIMS' AS source_table,
    'RAW_CLAIMS' AS upstream_source,
    ARRAY_CONSTRUCT(
        'V_CLAIMS_BY_REGION',
        'V_CLAIMS_MONTHLY_TREND',
        'V_HIGH_RISK_CLAIMS',
        'V_CLAIMS_WITH_POLICY',
        'AGG_CLAIMS_EXECUTIVE',
        'FRAUD_DETECTION_FEATURES'
    ) AS downstream_objects,
    'Curated claims data flows to 6 downstream analytics and ML objects' AS description
UNION ALL
SELECT 
    'DIM_POLICIES' AS source_table,
    'RAW_POLICIES' AS upstream_source,
    ARRAY_CONSTRUCT(
        'V_CLAIMS_WITH_POLICY',
        'FRAUD_DETECTION_FEATURES'
    ) AS downstream_objects,
    'Curated policy data enriches claims for analytics and ML' AS description;

-- ============================================================================
-- SECTION 7: DEMO SCRIPT - LINEAGE VISUALIZATION
-- ============================================================================

/*
DEMO WALKTHROUGH - Lineage Visualization

1. NAVIGATE TO LINEAGE IN SNOWSIGHT
   - Go to Data > Databases > INSURANCECO > ANALYTICS
   - Click on V_HIGH_RISK_CLAIMS
   - Click the "Lineage" tab

2. SHOW UPSTREAM LINEAGE
   - Point to DIM_CLAIMS upstream
   - Point out: "We can see this view gets data from DIM_CLAIMS"
   - Click on DIM_CLAIMS in the lineage graph
   - Show RAW_CLAIMS upstream
   - Point out: "Full traceability back to the source"

3. SHOW COLUMN-LEVEL LINEAGE
   - Click on a specific column (e.g., claim_amount)
   - Show the column-level lineage highlighting
   - Point out: "We can trace individual columns through transformations"
   - Point out: "fraud_flag flows from RAW → DIM_CLAIMS → V_HIGH_RISK_CLAIMS"

4. SHOW DOWNSTREAM LINEAGE
   - Navigate to DIM_CLAIMS
   - Show all the downstream objects that depend on it
   - Point out: "If DIM_CLAIMS has a quality issue, these are all impacted"
   - Point out: "This is critical for impact analysis"

5. CONNECT TO DATA QUALITY
   - While on DIM_CLAIMS lineage
   - Point out where DMF alerts would appear on the lineage
   - Point out: "Quality issues are visible in context of data flow"

6. SHOW ML PIPELINE LINEAGE
   - Navigate to FRAUD_DETECTION_FEATURES
   - Show it pulls from both DIM_CLAIMS and DIM_POLICIES
   - Point out: "Data scientists can see exactly where their features come from"
   - Point out: "Critical for model governance and regulatory compliance"

KEY TALKING POINTS:
- "Lineage is captured AUTOMATICALLY - no manual documentation"
- "Every SQL transformation creates lineage metadata"
- "Column-level means precise impact analysis"
- "Quality alerts appear in the context of data flow"
- "Essential for root cause analysis - 'why is this dashboard wrong?'"

THE WOW MOMENT:
Click on a column in the downstream view and watch the entire lineage
graph highlight the path of that specific column back to source.
*/

-- ============================================================================
-- SECTION 8: VERIFICATION
-- ============================================================================

-- Show all analytics views created
SHOW VIEWS IN SCHEMA INSURANCECO.ANALYTICS;

-- Show feature table
SHOW TABLES IN SCHEMA INSURANCECO.DATA_SCIENCE;

-- Preview lineage summary
SELECT * FROM INSURANCECO.GOVERNANCE.V_LINEAGE_SUMMARY;

-- Count downstream dependencies
SELECT 
    'Objects depending on DIM_CLAIMS' AS metric,
    6 AS count
UNION ALL
SELECT 
    'Objects depending on DIM_POLICIES' AS metric,
    2 AS count;

SELECT 'Lineage demonstration objects created!' AS STATUS,
       '4 analytics views created' AS VIEWS,
       '1 aggregate table created' AS AGGREGATES,
       '1 ML feature table created' AS ML_FEATURES,
       'Ready for lineage demonstration' AS NEXT_STEP;
