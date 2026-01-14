/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 04: Horizon Catalog Setup
================================================================================
Purpose: Configure Horizon Catalog features - ownership, stewardship, and 
         business metadata for data discovery
Author: Demo Setup Script
Date: 2025-01

VIGNETTE 1: Establishing a Single Source of Truth
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE GOVERNANCE_ADMIN;
USE WAREHOUSE INSURANCECO_ADMIN_WH;
USE DATABASE INSURANCECO;

-- ============================================================================
-- SECTION 2: ASSIGN DATA OWNERSHIP
-- ============================================================================

/*
 * Data Ownership establishes accountability for data assets.
 * In Horizon, ownership is shown in the catalog and drives stewardship workflows.
 */

-- Transfer ownership of curated schema to Data Engineering role
GRANT OWNERSHIP ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;

-- Transfer ownership of curated tables to Data Engineering role
GRANT OWNERSHIP ON TABLE INSURANCECO.CURATED.DIM_CLAIMS TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON TABLE INSURANCECO.CURATED.DIM_POLICIES TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;

-- ============================================================================
-- SECTION 3: CREATE SEMANTIC CATEGORIES (Custom Tags for Classification)
-- ============================================================================

USE SCHEMA INSURANCECO.GOVERNANCE;

-- Create semantic category tags for business classification
CREATE TAG IF NOT EXISTS SEMANTIC_CATEGORY
    ALLOWED_VALUES 'IDENTIFIER', 'MEASURE', 'ATTRIBUTE', 'DATE', 'FLAG', 'PII', 'SENSITIVE'
    COMMENT = 'Classifies columns by their semantic meaning in the data model';

CREATE TAG IF NOT EXISTS DATA_DOMAIN
    ALLOWED_VALUES 'CLAIMS', 'POLICIES', 'CUSTOMERS', 'VEHICLES', 'PAYMENTS', 'RISK'
    COMMENT = 'Business domain that owns this data asset';

CREATE TAG IF NOT EXISTS DATA_QUALITY_TIER
    ALLOWED_VALUES 'GOLD', 'SILVER', 'BRONZE', 'RAW'
    COMMENT = 'Data quality certification tier';

CREATE TAG IF NOT EXISTS DATA_STEWARD
    COMMENT = 'Email or name of the responsible data steward';

CREATE TAG IF NOT EXISTS REFRESH_FREQUENCY
    ALLOWED_VALUES 'REAL-TIME', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY', 'ON-DEMAND'
    COMMENT = 'How often this data is refreshed';

CREATE TAG IF NOT EXISTS GDPR_CLASSIFICATION
    ALLOWED_VALUES 'PERSONAL_DATA', 'SPECIAL_CATEGORY', 'NON_PERSONAL', 'PSEUDONYMIZED'
    COMMENT = 'GDPR data classification for compliance';

-- ============================================================================
-- SECTION 4: APPLY TAGS TO DIM_CLAIMS TABLE
-- ============================================================================

USE SCHEMA INSURANCECO.CURATED;

-- Apply table-level tags
ALTER TABLE DIM_CLAIMS SET TAG
    INSURANCECO.GOVERNANCE.DATA_DOMAIN = 'CLAIMS',
    INSURANCECO.GOVERNANCE.DATA_QUALITY_TIER = 'GOLD',
    INSURANCECO.GOVERNANCE.DATA_STEWARD = 'claims-team@insuranceco.dk',
    INSURANCECO.GOVERNANCE.REFRESH_FREQUENCY = 'DAILY';

-- Apply column-level tags for semantic classification
-- Primary keys and identifiers
ALTER TABLE DIM_CLAIMS MODIFY COLUMN claim_key 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'IDENTIFIER';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN claim_id 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'IDENTIFIER';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_id 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'IDENTIFIER';

-- Financial measures
ALTER TABLE DIM_CLAIMS MODIFY COLUMN claim_amount 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'MEASURE';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_coverage_limit 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'MEASURE';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN coverage_utilization_pct 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'MEASURE';

-- Date columns
ALTER TABLE DIM_CLAIMS MODIFY COLUMN date_of_incident 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'DATE';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN date_reported 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'DATE';

-- Flag columns
ALTER TABLE DIM_CLAIMS MODIFY COLUMN fraud_flag 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'FLAG';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN exceeds_coverage 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'FLAG';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN high_value_claim 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'FLAG';

-- PII columns (will also get masking tags in script 06)
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_name 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_email 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN policy_holder_cpr 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'SENSITIVE',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'SPECIAL_CATEGORY';
ALTER TABLE DIM_CLAIMS MODIFY COLUMN address 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';

-- ============================================================================
-- SECTION 5: APPLY TAGS TO DIM_POLICIES TABLE
-- ============================================================================

-- Apply table-level tags
ALTER TABLE DIM_POLICIES SET TAG
    INSURANCECO.GOVERNANCE.DATA_DOMAIN = 'POLICIES',
    INSURANCECO.GOVERNANCE.DATA_QUALITY_TIER = 'GOLD',
    INSURANCECO.GOVERNANCE.DATA_STEWARD = 'underwriting-team@insuranceco.dk',
    INSURANCECO.GOVERNANCE.REFRESH_FREQUENCY = 'DAILY';

-- PII columns
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_name 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_email 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';
ALTER TABLE DIM_POLICIES MODIFY COLUMN policy_holder_cpr 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'SENSITIVE',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'SPECIAL_CATEGORY';
ALTER TABLE DIM_POLICIES MODIFY COLUMN address 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'PII',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';
ALTER TABLE DIM_POLICIES MODIFY COLUMN vehicle_vin 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'SENSITIVE',
        INSURANCECO.GOVERNANCE.GDPR_CLASSIFICATION = 'PERSONAL_DATA';

-- Risk columns
ALTER TABLE DIM_POLICIES MODIFY COLUMN risk_score 
    SET TAG INSURANCECO.GOVERNANCE.SEMANTIC_CATEGORY = 'ATTRIBUTE',
        INSURANCECO.GOVERNANCE.DATA_DOMAIN = 'RISK';

-- ============================================================================
-- SECTION 6: TAG RAW TABLES AS BRONZE TIER
-- ============================================================================

USE SCHEMA INSURANCECO.RAW;

ALTER TABLE RAW_CLAIMS SET TAG
    INSURANCECO.GOVERNANCE.DATA_DOMAIN = 'CLAIMS',
    INSURANCECO.GOVERNANCE.DATA_QUALITY_TIER = 'BRONZE',
    INSURANCECO.GOVERNANCE.REFRESH_FREQUENCY = 'DAILY';

ALTER TABLE RAW_POLICIES SET TAG
    INSURANCECO.GOVERNANCE.DATA_DOMAIN = 'POLICIES',
    INSURANCECO.GOVERNANCE.DATA_QUALITY_TIER = 'BRONZE',
    INSURANCECO.GOVERNANCE.REFRESH_FREQUENCY = 'DAILY';

-- ============================================================================
-- SECTION 7: CREATE CATALOG VIEWS FOR DISCOVERY
-- ============================================================================

USE SCHEMA INSURANCECO.GOVERNANCE;

select *  FROM INSURANCECO.INFORMATION_SCHEMA.TABLES ;


-- View to show all tagged tables with their classification
CREATE OR REPLACE VIEW V_DATA_CATALOG AS
SELECT
    t.table_catalog AS database_name,
    t.table_schema AS schema_name,
    t.table_name,
    t.table_type,
    t.row_count,
    t.bytes / (1024*1024) AS size_mb,
    t.comment AS table_description,
    -- Get tags
    MAX(CASE WHEN tr.tag_name = 'DATA_DOMAIN' THEN tr.tag_value END) AS data_domain,
    MAX(CASE WHEN tr.tag_name = 'DATA_QUALITY_TIER' THEN tr.tag_value END) AS quality_tier,
    MAX(CASE WHEN tr.tag_name = 'DATA_STEWARD' THEN tr.tag_value END) AS data_steward,
    MAX(CASE WHEN tr.tag_name = 'REFRESH_FREQUENCY' THEN tr.tag_value END) AS refresh_frequency,
    t.created AS created_at,
    t.last_altered AS last_modified
FROM INSURANCECO.INFORMATION_SCHEMA.TABLES t
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
    ON t.table_catalog = tr.object_database
    AND t.table_schema = tr.object_schema
    AND t.table_name = tr.object_name
    AND tr.domain = 'TABLE'
WHERE t.table_schema IN ('RAW', 'CURATED', 'ANALYTICS')
GROUP BY 1,2,3,4,5,6,7,12,13;

-- View to show PII columns across the organization
CREATE OR REPLACE VIEW V_PII_INVENTORY AS
SELECT
    tr.object_database AS database_name,
    tr.object_schema AS schema_name,
    tr.object_name AS table_name,
    tr.column_name,
    tr.tag_value AS classification,
    MAX(CASE WHEN tr2.tag_name = 'GDPR_CLASSIFICATION' THEN tr2.tag_value END) AS gdpr_status
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr2
    ON tr.object_database = tr2.object_database
    AND tr.object_schema = tr2.object_schema
    AND tr.object_name = tr2.object_name
    AND tr.column_name = tr2.column_name
    AND tr2.tag_name = 'GDPR_CLASSIFICATION'
WHERE tr.tag_name = 'SEMANTIC_CATEGORY'
  AND tr.tag_value IN ('PII', 'SENSITIVE')
  AND tr.domain = 'COLUMN'
GROUP BY 1,2,3,4,5;

-- ============================================================================
-- SECTION 8: VERIFICATION - CATALOG DISCOVERY QUERIES
-- ============================================================================

-- Show all tags defined
SHOW TAGS IN SCHEMA INSURANCECO.GOVERNANCE;

-- Show tags applied to DIM_CLAIMS table
SELECT * FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'INSURANCECO.CURATED.DIM_CLAIMS', 'TABLE'
));

-- Query the catalog view
-- SELECT * FROM V_DATA_CATALOG;

-- Query PII inventory
-- SELECT * FROM V_PII_INVENTORY;

-- ============================================================================
-- SECTION 9: DEMO SCRIPT - HORIZON CATALOG DISCOVERY
-- ============================================================================

/*
DEMO WALKTHROUGH - Vignette 1: Single Source of Truth

1. START IN SNOWSIGHT
   - Navigate to Data > Databases > INSURANCECO
   - Show the RAW schema with undocumented tables
   - Point out: "This is what most organizations start with - raw, undocumented data"

2. USE HORIZON CATALOG SEARCH
   - Click the Search icon (magnifying glass) in Snowsight
   - Type: "claims"
   - Show the search results with DIM_CLAIMS appearing
   - Point out: "Notice how the catalog shows quality tier, domain, and steward"

3. EXPLORE DIM_CLAIMS
   - Click on DIM_CLAIMS to open the detail view
   - Show the table description/comment
   - Show column comments (hover over columns)
   - Show the tags (PII classification, GDPR status)
   - Point out: "All this metadata lives WITH the data - always in sync"

4. SHOW COLUMN DETAILS
   - Click on a column like POLICY_HOLDER_NAME
   - Show the comment: "Full name of the policy holder - CONTAINS PII"
   - Show the tags: SEMANTIC_CATEGORY = PII, GDPR_CLASSIFICATION = PERSONAL_DATA
   - Point out: "This classification automatically drives masking policies"

5. DEMONSTRATE QUICK QUERY
   - From the table detail view, click "Query Data"
   - Show how it opens a worksheet with the table pre-selected
   - Point out: "One click from discovery to analysis"

6. SHOW LINEAGE (Preview)
   - While on DIM_CLAIMS, click the "Lineage" tab
   - Show the connection from RAW_CLAIMS → DIM_CLAIMS
   - Point out: "We'll dive deeper into lineage in the next vignette"

KEY TALKING POINTS:
- "The catalog is built-in - no separate tool to maintain"
- "Metadata is always real-time because it lives with the data"
- "Tags drive policies - classify once, protect everywhere"
- "New team members can become productive in hours, not weeks"
*/

SELECT 'Horizon Catalog setup complete!' AS STATUS,
       'Tags created and applied' AS TAGS,
       'Ready for catalog demonstration' AS NEXT_STEP;
