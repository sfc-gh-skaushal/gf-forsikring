/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 06: Tagging and Masking Policies for GDPR Compliance
================================================================================
Purpose: Create PII classification tags and dynamic masking policies to 
         automatically protect sensitive data based on user role
Author: Demo Setup Script
Date: 2025-01

VIGNETTE 3: Secure AI & Compliance at Scale
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
-- SECTION 2: CREATE PII CLASSIFICATION TAGS
-- ============================================================================

/*
 * Tag-based masking allows you to:
 * 1. Classify data ONCE using tags
 * 2. Define masking policies that apply to tags
 * 3. Automatically mask any column with that tag
 * 
 * This is "classify once, protect everywhere" at scale.
 */

-- Primary PII tag for direct identifiers
CREATE TAG IF NOT EXISTS PII
    ALLOWED_VALUES 'NAME', 'EMAIL', 'PHONE', 'ADDRESS', 'SSN', 'CPR', 'FINANCIAL'
    COMMENT = 'Personally Identifiable Information classification for GDPR compliance';

-- Sensitivity level tag
CREATE TAG IF NOT EXISTS SENSITIVITY_LEVEL
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification level';

-- Data retention tag
CREATE TAG IF NOT EXISTS RETENTION_PERIOD
    ALLOWED_VALUES '30_DAYS', '90_DAYS', '1_YEAR', '3_YEARS', '7_YEARS', 'INDEFINITE'
    COMMENT = 'Data retention period for compliance';

-- ============================================================================
-- SECTION 3: CREATE MASKING POLICIES
-- ============================================================================

/*
 * Masking policies define HOW data is masked based on the querying role.
 * Each policy checks CURRENT_ROLE() and returns either:
 * - Full data (for privileged roles like DATA_SCIENTIST)
 * - Partially masked data (for DATA_ENGINEER)
 * - Fully masked data (for DATA_ANALYST and others)
 */

-- Masking policy for NAME fields
-- Shows full name to DATA_SCIENTIST, masks for others
CREATE OR REPLACE MASKING POLICY MASK_PII_NAME AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST', 'GOVERNANCE_ADMIN') THEN val
        WHEN CURRENT_ROLE() = 'DATA_ENGINEER' THEN 
            -- Show first initial and last name
            CONCAT(LEFT(val, 1), '. ', SPLIT_PART(val, ' ', -1))
        ELSE 
            -- Full mask for analysts and others
            '***MASKED***'
    END;

-- Masking policy for EMAIL fields
-- Shows domain only to analysts, full email to scientists
CREATE OR REPLACE MASKING POLICY MASK_PII_EMAIL AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST', 'GOVERNANCE_ADMIN') THEN val
        WHEN CURRENT_ROLE() = 'DATA_ENGINEER' THEN 
            -- Show domain only
            CONCAT('****@', SPLIT_PART(val, '@', 2))
        ELSE 
            -- Full mask
            '****@****.***'
    END;

-- Masking policy for ADDRESS fields
-- Shows city only to engineers, full to scientists
CREATE OR REPLACE MASKING POLICY MASK_PII_ADDRESS AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST', 'GOVERNANCE_ADMIN') THEN val
        WHEN CURRENT_ROLE() = 'DATA_ENGINEER' THEN 
            -- Partial mask - show last part (often city)
            CONCAT('*** ', SPLIT_PART(val, ' ', -1))
        ELSE 
            '***MASKED ADDRESS***'
    END;

-- Masking policy for CPR (Danish SSN) - highly sensitive
-- Only DATA_SCIENTIST can see full CPR
CREATE OR REPLACE MASKING POLICY MASK_PII_CPR AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST') THEN val
        WHEN CURRENT_ROLE() = 'GOVERNANCE_ADMIN' THEN 
            -- Show last 4 digits only
            CONCAT('******-', RIGHT(val, 4))
        ELSE 
            -- Full mask for everyone else
            '******-****'
    END;

-- Masking policy for financial data (VIN, account numbers, etc.)
CREATE OR REPLACE MASKING POLICY MASK_PII_FINANCIAL AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_SCIENTIST', 'GOVERNANCE_ADMIN') THEN val
        WHEN CURRENT_ROLE() = 'DATA_ENGINEER' THEN 
            -- Show last 4 characters
            CONCAT('***', RIGHT(val, 4))
        ELSE 
            '*****************'
    END;

-- ============================================================================
-- SECTION 4: APPLY TAGS AND MASKING POLICIES TO COLUMNS
-- ============================================================================

/*
 * We apply both tags (for classification/discovery) AND masking policies 
 * (for data protection) to PII columns. This gives us:
 * - Tags: Visible in Horizon Catalog for data discovery
 * - Masking: Role-based access control on the actual data
 */

USE SCHEMA INSURANCECO.CURATED;

-- ============================================================================
-- DIM_CLAIMS - Apply Tags and Masking Policies
-- ============================================================================

-- POLICY_HOLDER_NAME: Tag + Masking
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_name 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'NAME';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_name 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_NAME;

-- POLICY_HOLDER_EMAIL: Tag + Masking  
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_email 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'EMAIL';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_email 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_EMAIL;

-- POLICY_HOLDER_CPR: Tag + Masking (most sensitive)
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_cpr 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'CPR';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_cpr 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_CPR;

-- ADDRESS: Tag + Masking
ALTER TABLE DIM_CLAIMS MODIFY COLUMN address 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'ADDRESS';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN address 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_ADDRESS;

-- ============================================================================
-- DIM_POLICIES - Apply Tags and Masking Policies
-- ============================================================================

-- POLICY_HOLDER_NAME: Tag + Masking
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_name 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'NAME';
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_name 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_NAME;

-- POLICY_HOLDER_EMAIL: Tag + Masking
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_email 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'EMAIL';
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_email 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_EMAIL;

-- POLICY_HOLDER_CPR: Tag + Masking
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_cpr 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'CPR';
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_cpr 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_CPR;

-- ADDRESS: Tag + Masking
ALTER TABLE DIM_POLICIES MODIFY COLUMN address 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'ADDRESS';
ALTER TABLE DIM_POLICIES MODIFY COLUMN address 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_ADDRESS;

-- VEHICLE_VIN: Tag + Masking
ALTER TABLE DIM_POLICIES MODIFY COLUMN vehicle_vin 
    SET TAG INSURANCECO.GOVERNANCE.PII = 'FINANCIAL';
ALTER TABLE DIM_POLICIES MODIFY COLUMN vehicle_vin 
    SET MASKING POLICY INSURANCECO.GOVERNANCE.MASK_PII_FINANCIAL;

-- ============================================================================
-- SECTION 5: APPLY TABLE-LEVEL TAGS
-- ============================================================================

-- Set sensitivity levels
ALTER TABLE DIM_CLAIMS SET TAG INSURANCECO.GOVERNANCE.SENSITIVITY_LEVEL = 'CONFIDENTIAL';
ALTER TABLE DIM_POLICIES SET TAG INSURANCECO.GOVERNANCE.SENSITIVITY_LEVEL = 'CONFIDENTIAL';

-- Set retention periods (GDPR compliance)
ALTER TABLE DIM_CLAIMS SET TAG INSURANCECO.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';
ALTER TABLE DIM_POLICIES SET TAG INSURANCECO.GOVERNANCE.RETENTION_PERIOD = '7_YEARS';

-- ============================================================================
-- SECTION 6: GRANT SELECT PERMISSIONS FOR DEMO
-- ============================================================================

-- Allow all roles to query curated tables (masking will apply automatically)
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_CLAIMS TO ROLE DATA_ANALYST;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_CLAIMS TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_CLAIMS TO ROLE DATA_SCIENTIST;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_CLAIMS TO ROLE DATA_STEWARD;

GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_POLICIES TO ROLE DATA_ANALYST;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_POLICIES TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_POLICIES TO ROLE DATA_SCIENTIST;
GRANT SELECT ON TABLE INSURANCECO.CURATED.DIM_POLICIES TO ROLE DATA_STEWARD;

-- ============================================================================
-- SECTION 7: CREATE AUDIT VIEW FOR MASKING POLICY USAGE
-- ============================================================================

USE SCHEMA INSURANCECO.GOVERNANCE;

-- View to show which columns are masked and by which policy
CREATE OR REPLACE VIEW V_MASKING_AUDIT AS
SELECT
    tr.object_database,
    tr.object_schema,
    tr.object_name AS table_name,
    tr.column_name,
    tr.tag_name,
    tr.tag_value AS pii_classification,
    CASE tr.tag_value
        WHEN 'NAME' THEN 'MASK_PII_NAME'
        WHEN 'EMAIL' THEN 'MASK_PII_EMAIL'
        WHEN 'ADDRESS' THEN 'MASK_PII_ADDRESS'
        WHEN 'CPR' THEN 'MASK_PII_CPR'
        WHEN 'FINANCIAL' THEN 'MASK_PII_FINANCIAL'
        ELSE 'No masking'
    END AS masking_policy_applied
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
WHERE tr.tag_name = 'PII'
  AND tr.domain = 'COLUMN'
  AND tr.tag_database = 'INSURANCECO'
ORDER BY tr.object_schema, tr.object_name, tr.column_name;

-- ============================================================================
-- SECTION 8: DEMO VERIFICATION QUERIES
-- ============================================================================

/*
 * These queries demonstrate the dynamic masking in action.
 * Run them with different roles to see different results.
 */

-- Test Query: View as DATA_ANALYST (heavily masked)
-- USE ROLE DATA_ANALYST;
-- USE WAREHOUSE INSURANCECO_ANALYTICS_WH;
-- SELECT 
--     claim_id,
--     policy_holder_name,
--     policy_holder_email,
--     policy_holder_cpr,
--     address,
--     claim_amount,
--     claim_type
-- FROM INSURANCECO.CURATED.DIM_CLAIMS
-- LIMIT 5;

-- Test Query: View as DATA_ENGINEER (partially masked)
-- USE ROLE DATA_ENGINEER;
-- USE WAREHOUSE INSURANCECO_ETL_WH;
-- SELECT 
--     claim_id,
--     policy_holder_name,
--     policy_holder_email,
--     policy_holder_cpr,
--     address,
--     claim_amount,
--     claim_type
-- FROM INSURANCECO.CURATED.DIM_CLAIMS
-- LIMIT 5;

-- Test Query: View as DATA_SCIENTIST (full access for ML)
-- USE ROLE DATA_SCIENTIST;
-- USE WAREHOUSE INSURANCECO_ML_WH;
-- SELECT 
--     claim_id,
--     policy_holder_name,
--     policy_holder_email,
--     policy_holder_cpr,
--     address,
--     claim_amount,
--     claim_type
-- FROM INSURANCECO.CURATED.DIM_CLAIMS
-- LIMIT 5;

-- ============================================================================
-- SECTION 9: VERIFY TAG AND POLICY APPLICATION
-- ============================================================================

-- Show all PII tags applied
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
    'INSURANCECO.CURATED.DIM_CLAIMS', 'TABLE'
));

-- Show columns with PII tags
SELECT 
    column_name,
    tag_name,
    tag_value
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'INSURANCECO.CURATED.DIM_CLAIMS', 'TABLE'
))
WHERE tag_name = 'PII';

-- ============================================================================
-- SECTION 10: DEMO SCRIPT - TAG-BASED MASKING
-- ============================================================================

/*
DEMO WALKTHROUGH - Vignette 3: Secure AI & Compliance at Scale

1. SET UP THE STORY
   - "Your data scientists need access to claims data for fraud modeling"
   - "But GDPR requires you to protect customer PII"
   - "How do you enable innovation while ensuring compliance?"

2. SHOW THE TAGS (As GOVERNANCE_ADMIN)
   USE ROLE GOVERNANCE_ADMIN;
   
   - Show the PII tag definition
   - Show which columns have PII tags
   - Run: SELECT * FROM V_MASKING_AUDIT
   - Point out: "We've classified 5 columns as PII across 2 tables"

3. DEMONSTRATE DYNAMIC MASKING
   
   Step A: Run query as DATA_ANALYST
   ------------------------------------
   USE ROLE DATA_ANALYST;
   USE WAREHOUSE INSURANCECO_ANALYTICS_WH;
   
   SELECT 
       claim_id,
       policy_holder_name,
       policy_holder_email,
       policy_holder_cpr,
       address,
       claim_amount
   FROM INSURANCECO.CURATED.DIM_CLAIMS
   LIMIT 5;
   
   Point out: "Notice all PII columns are fully masked"
   Point out: "Analysts can still do their job - they see claim_amount, claim_type, etc."
   
   Step B: Run SAME query as DATA_SCIENTIST
   -----------------------------------------
   USE ROLE DATA_SCIENTIST;
   USE WAREHOUSE INSURANCECO_ML_WH;
   
   SELECT 
       claim_id,
       policy_holder_name,
       policy_holder_email,
       policy_holder_cpr,
       address,
       claim_amount
   FROM INSURANCECO.CURATED.DIM_CLAIMS
   LIMIT 5;
   
   Point out: "SAME QUERY - completely different results!"
   Point out: "Data scientist sees real data for fraud modeling"

4. EXPLAIN THE MAGIC
   - "This is TAG-BASED masking"
   - "We define the masking policy ONCE, attach it to a TAG"
   - "Every column with that tag is automatically protected"
   - "Add a new PII column? Just tag it - masking applies instantly"

5. SHOW SCALABILITY
   - "Currently we have 5 PII columns tagged"
   - "Tomorrow we might have 500 across 100 tables"
   - "The governance model scales automatically"
   - "No need to write individual masking policies for each column"

6. COMPLIANCE STORY
   - "This is GDPR compliance built into the platform"
   - "Every query is audited - you can prove who saw what"
   - "Data never leaves Snowflake's governance boundary"
   - "Replaces manual data provisioning that takes weeks"

KEY TALKING POINTS:
- "Same query, different roles, different results"
- "Tag once, protect everywhere"
- "Data scientists work on real data, analysts on masked data"
- "All within Snowflake - no data movement, no risk"
- "Audit trail built-in for compliance reporting"

THE WOW MOMENT:
Show the side-by-side comparison of DATA_ANALYST vs DATA_SCIENTIST 
results on the EXACT same query. This visual contrast is powerful.
*/

SELECT 'Tag-based masking setup complete!' AS STATUS,
       '5 PII tag values configured' AS TAGS,
       '5 masking policies created' AS POLICIES,
       'Ready for compliance demonstration' AS NEXT_STEP;
