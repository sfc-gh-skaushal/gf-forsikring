/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 03: Create Curated Tables
================================================================================
Purpose: Transform raw data into curated, documented dimension tables
Author: Demo Setup Script
Date: 2025-01
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE DATA_ENGINEER;
USE WAREHOUSE INSURANCECO_ETL_WH;
USE DATABASE INSURANCECO;
USE SCHEMA CURATED;

-- ============================================================================
-- SECTION 2: CREATE DIM_CLAIMS - Main Claims Dimension
-- ============================================================================

CREATE OR REPLACE TABLE DIM_CLAIMS (
    -- Primary Key
    claim_key NUMBER AUTOINCREMENT PRIMARY KEY
        COMMENT 'Surrogate key for the claims dimension',
    
    -- Business Keys
    claim_id VARCHAR(20) NOT NULL
        COMMENT 'Unique business identifier for the claim (e.g., CLM-2025-00001)',
    policy_id VARCHAR(20) NOT NULL
        COMMENT 'Reference to the associated insurance policy',
    
    -- Claim Financial Details
    claim_amount NUMBER(12,2)
        COMMENT 'Amount claimed in Danish Kroner (DKK)',
    policy_coverage_limit NUMBER(12,2)
        COMMENT 'Maximum coverage amount for the policy in DKK',
    coverage_utilization_pct NUMBER(5,2)
        COMMENT 'Percentage of coverage used by this claim',
    
    -- Claim Dates
    date_of_incident DATE
        COMMENT 'Date when the incident occurred',
    date_reported DATE
        COMMENT 'Date when the claim was reported to InsuranceCo',
    days_to_report NUMBER(5)
        COMMENT 'Number of days between incident and reporting',
    
    -- Claim Classification
    claim_type VARCHAR(50)
        COMMENT 'Type of claim: collision, theft, vandalism, weather, glass, fire',
    claim_status VARCHAR(50)
        COMMENT 'Current status: pending, under_review, approved, flagged',
    
    -- Customer PII (Will be masked for non-privileged roles)
    policy_holder_name VARCHAR(200)
        COMMENT 'Full name of the policy holder - CONTAINS PII',
    policy_holder_email VARCHAR(200)
        COMMENT 'Email address of the policy holder - CONTAINS PII',
    policy_holder_cpr VARCHAR(20)
        COMMENT 'Danish CPR number (personnummer) - CONTAINS SENSITIVE PII',
    
    -- Location Information
    address VARCHAR(500)
        COMMENT 'Street address of the policy holder - CONTAINS PII',
    city VARCHAR(100)
        COMMENT 'City of residence',
    postal_code VARCHAR(10)
        COMMENT 'Danish postal code',
    region VARCHAR(50)
        COMMENT 'Geographic region of Denmark',
    
    -- Vehicle Information
    vehicle_make VARCHAR(50)
        COMMENT 'Manufacturer of the insured vehicle',
    vehicle_model VARCHAR(50)
        COMMENT 'Model of the insured vehicle',
    vehicle_year NUMBER(4)
        COMMENT 'Year of manufacture',
    vehicle_age NUMBER(3)
        COMMENT 'Age of vehicle in years at time of claim',
    
    -- Claim Details
    damage_description VARCHAR(1000)
        COMMENT 'Description of damage from adjuster assessment',
    adjuster_notes VARCHAR(2000)
        COMMENT 'Internal notes from claims adjuster',
    
    -- Risk & Fraud Indicators
    fraud_flag BOOLEAN
        COMMENT 'TRUE if claim has been flagged for potential fraud investigation',
    exceeds_coverage BOOLEAN
        COMMENT 'TRUE if claim amount exceeds policy coverage limit',
    high_value_claim BOOLEAN
        COMMENT 'TRUE if claim amount exceeds 100,000 DKK threshold',
    
    -- Audit & Metadata
    created_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Timestamp when record was created in curated layer',
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Timestamp of last update',
    source_system VARCHAR(50) DEFAULT 'CLAIMS_SYSTEM'
        COMMENT 'Source system identifier',
    data_quality_score NUMBER(3,2)
        COMMENT 'Data quality score from 0.00 to 1.00'
)
COMMENT = 'Curated claims dimension table - the trusted source for claims analytics. Contains policy holder PII - access controlled via masking policies.'
;

--alter table DIM_CLAIMS modify created_at TIMESTAMP_LTZ;

-- ============================================================================
-- SECTION 3: POPULATE DIM_CLAIMS FROM RAW
-- ============================================================================

INSERT INTO DIM_CLAIMS (
    claim_id, policy_id, claim_amount, policy_coverage_limit, coverage_utilization_pct,
    date_of_incident, date_reported, days_to_report,
    claim_type, claim_status,
    policy_holder_name, policy_holder_email, policy_holder_cpr,
    address, city, postal_code, region,
    vehicle_make, vehicle_model, vehicle_year, vehicle_age,
    damage_description, adjuster_notes,
    fraud_flag, exceeds_coverage, high_value_claim,
    data_quality_score
)
SELECT
    claim_id,
    policy_id,
    claim_amount,
    policy_coverage_limit,
    ROUND((claim_amount / NULLIF(policy_coverage_limit, 0)) * 100, 2) AS coverage_utilization_pct,
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
    -- Derive region from postal code
    CASE 
        WHEN LEFT(postal_code, 1) IN ('1', '2') THEN 'Greater Copenhagen'
        WHEN LEFT(postal_code, 1) = '3' THEN 'North Zealand'
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
    -- Calculate data quality score (example logic)
    CASE 
        WHEN claim_id IS NOT NULL 
             AND policy_id IS NOT NULL 
             AND claim_amount IS NOT NULL 
             AND date_of_incident IS NOT NULL
        THEN 1.00
        ELSE 0.75
    END AS data_quality_score
FROM INSURANCECO.RAW.RAW_CLAIMS;

-- ============================================================================
-- SECTION 4: CREATE DIM_POLICIES - Policy Dimension
-- ============================================================================

CREATE OR REPLACE TABLE DIM_POLICIES (
    -- Primary Key
    policy_key NUMBER AUTOINCREMENT PRIMARY KEY
        COMMENT 'Surrogate key for the policy dimension',
    
    -- Business Key
    policy_id VARCHAR(20) NOT NULL UNIQUE
        COMMENT 'Unique business identifier for the policy',
    
    -- Customer PII
    policy_holder_name VARCHAR(200)
        COMMENT 'Full name of the policy holder - CONTAINS PII',
    policy_holder_email VARCHAR(200)
        COMMENT 'Email address - CONTAINS PII',
    policy_holder_cpr VARCHAR(20)
        COMMENT 'Danish CPR number - CONTAINS SENSITIVE PII',
    
    -- Location
    address VARCHAR(500)
        COMMENT 'Street address - CONTAINS PII',
    city VARCHAR(100)
        COMMENT 'City of residence',
    postal_code VARCHAR(10)
        COMMENT 'Danish postal code',
    region VARCHAR(50)
        COMMENT 'Geographic region',
    
    -- Policy Details
    policy_type VARCHAR(50)
        COMMENT 'Policy type: basic, comprehensive, premium',
    coverage_limit NUMBER(12,2)
        COMMENT 'Maximum coverage amount in DKK',
    premium_annual NUMBER(10,2)
        COMMENT 'Annual premium in DKK',
    policy_start_date DATE
        COMMENT 'Policy effective start date',
    policy_end_date DATE
        COMMENT 'Policy expiration date',
    policy_term_months NUMBER(3)
        COMMENT 'Policy term in months',
    is_active BOOLEAN
        COMMENT 'TRUE if policy is currently active',
    
    -- Vehicle Details
    vehicle_make VARCHAR(50)
        COMMENT 'Vehicle manufacturer',
    vehicle_model VARCHAR(50)
        COMMENT 'Vehicle model',
    vehicle_year NUMBER(4)
        COMMENT 'Year of manufacture',
    vehicle_vin VARCHAR(50)
        COMMENT 'Vehicle Identification Number - SENSITIVE',
    
    -- Risk Assessment
    driver_age NUMBER(3)
        COMMENT 'Age of primary driver',
    years_licensed NUMBER(3)
        COMMENT 'Years driver has been licensed',
    previous_claims_count NUMBER(5)
        COMMENT 'Number of previous claims on record',
    risk_score VARCHAR(20)
        COMMENT 'Risk classification: LOW, MEDIUM, HIGH',
    
    -- Audit
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Record creation timestamp',
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Last update timestamp'
)
COMMENT = 'Curated policy dimension - master data for insurance policies. Contains PII - access controlled.'
;

-- ============================================================================
-- SECTION 5: POPULATE DIM_POLICIES
-- ============================================================================

INSERT INTO DIM_POLICIES (
    policy_id, policy_holder_name, policy_holder_email, policy_holder_cpr,
    address, city, postal_code, region,
    policy_type, coverage_limit, premium_annual,
    policy_start_date, policy_end_date, policy_term_months, is_active,
    vehicle_make, vehicle_model, vehicle_year, vehicle_vin,
    driver_age, years_licensed, previous_claims_count, risk_score
)
SELECT
    policy_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    city,
    postal_code,
    CASE 
        WHEN LEFT(postal_code, 1) IN ('1', '2') THEN 'Greater Copenhagen'
        WHEN LEFT(postal_code, 1) = '3' THEN 'North Zealand'
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
    DATEDIFF('month', policy_start_date, policy_end_date) AS policy_term_months,
    CURRENT_DATE() BETWEEN policy_start_date AND policy_end_date AS is_active,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_vin,
    driver_age,
    years_licensed,
    previous_claims_count,
    risk_score
FROM INSURANCECO.RAW.RAW_POLICIES;

-- ============================================================================
-- SECTION 6: ADD TABLE-LEVEL DOCUMENTATION
-- ============================================================================

-- Add detailed table comments
ALTER TABLE DIM_CLAIMS SET COMMENT = 
'CURATED CLAIMS DIMENSION TABLE
================================
Purpose: Trusted source for all claims analytics, reporting, and ML models.
Owner: Data Engineering Team
Steward: Claims Operations

Data Quality:
- All records validated against policy master
- Fraud flags reviewed by Claims Adjusters
- PII columns protected by masking policies

Usage Guidelines:
- Use for fraud detection modeling (DATA_SCIENTIST role)
- Use for claims dashboards (DATA_ANALYST role - PII masked)
- Do not use RAW_CLAIMS directly for analytics

Refresh: Daily incremental load from RAW.RAW_CLAIMS
SLA: Available by 06:00 CET daily';

ALTER TABLE DIM_POLICIES SET COMMENT = 
'CURATED POLICY DIMENSION TABLE
================================
Purpose: Master data for insurance policies - the source of truth for policy information.
Owner: Data Engineering Team
Steward: Underwriting Operations

Data Quality:
- Risk scores validated against underwriting rules
- Coverage limits verified against product catalog
- Active flag computed from policy dates

Usage Guidelines:
- Join with DIM_CLAIMS for claims analysis
- Use for customer segmentation
- PII protected by masking policies

Refresh: Daily full refresh from RAW.RAW_POLICIES
SLA: Available by 05:00 CET daily';

-- ============================================================================
-- SECTION 7: CREATE CONSTRAINTS AND INDEXES (Clustering)
-- ============================================================================

-- Add clustering for common query patterns
ALTER TABLE DIM_CLAIMS CLUSTER BY (date_reported, claim_type, claim_status);
ALTER TABLE DIM_POLICIES CLUSTER BY (policy_type, risk_score, region);

-- ============================================================================
-- SECTION 8: VERIFICATION
-- ============================================================================

-- Verify row counts
SELECT 'DIM_CLAIMS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DIM_CLAIMS
UNION ALL
SELECT 'DIM_POLICIES' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DIM_POLICIES;

-- Preview curated claims with calculated fields
SELECT 
    claim_id,
    policy_id,
    claim_amount,
    policy_coverage_limit,
    coverage_utilization_pct,
    days_to_report,
    region,
    vehicle_age,
    fraud_flag,
    exceeds_coverage,
    high_value_claim
FROM DIM_CLAIMS
LIMIT 10;

-- Show flagged claims for fraud investigation
SELECT 
    claim_id,
    claim_amount,
    policy_coverage_limit,
    exceeds_coverage,
    adjuster_notes
FROM DIM_CLAIMS
WHERE fraud_flag = TRUE;

SELECT 'Curated tables created successfully!' AS STATUS,
       (SELECT COUNT(*) FROM DIM_CLAIMS) AS CLAIMS_COUNT,
       (SELECT COUNT(*) FROM DIM_POLICIES) AS POLICIES_COUNT;
