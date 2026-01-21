/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 01: Environment Setup
================================================================================
Purpose: Create database, schemas, warehouses, and roles for the demo
Author: Demo Setup Script
Date: 2025-01
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE SYSADMIN;

-- ============================================================================
-- SECTION 2: CREATE WAREHOUSES
-- ============================================================================

-- Warehouse for governance and admin tasks
CREATE WAREHOUSE IF NOT EXISTS INSURANCECO_ADMIN_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for governance and administrative tasks';

-- Warehouse for data engineering workloads
CREATE WAREHOUSE IF NOT EXISTS INSURANCECO_ETL_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ETL and data transformation workloads';

-- Warehouse for analytics and BI
CREATE WAREHOUSE IF NOT EXISTS INSURANCECO_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for analytics and BI queries';

-- Warehouse for data science / ML workloads (Snowpark)
CREATE WAREHOUSE IF NOT EXISTS INSURANCECO_ML_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for ML and Snowpark workloads';

-- ============================================================================
-- SECTION 3: CREATE DATABASE AND SCHEMAS
-- ============================================================================

-- Main database for InsuranceCo
CREATE DATABASE IF NOT EXISTS INSURANCECO
    COMMENT = 'Main database for InsuranceCo insurance data platform';

-- Schema for raw/landing data
CREATE SCHEMA IF NOT EXISTS INSURANCECO.RAW
    COMMENT = 'Raw landing zone for ingested data - untransformed';

-- Schema for curated/governed data
CREATE SCHEMA IF NOT EXISTS INSURANCECO.CURATED
    COMMENT = 'Curated and governed data layer - production ready';

-- Schema for analytics and reporting views
CREATE SCHEMA IF NOT EXISTS INSURANCECO.ANALYTICS
    COMMENT = 'Analytics layer with aggregated views and marts';

-- Schema for data science / ML artifacts
CREATE SCHEMA IF NOT EXISTS INSURANCECO.DATA_SCIENCE
    COMMENT = 'Data science schema for ML models and features';

-- Schema for governance objects (tags, policies, DMFs)
CREATE SCHEMA IF NOT EXISTS INSURANCECO.GOVERNANCE
    COMMENT = 'Governance objects - tags, policies, and data quality functions';

-- ============================================================================
-- SECTION 4: CREATE CUSTOM ROLES
-- ============================================================================

use role accountadmin;

-- Governance Administrator - manages tags, policies, data quality
CREATE ROLE IF NOT EXISTS GOVERNANCE_ADMIN
    COMMENT = 'Administers data governance - tags, policies, quality metrics';

-- Data Engineer - builds pipelines and transformations
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'Builds and maintains data pipelines and transformations';

-- Data Scientist - ML and advanced analytics
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST
    COMMENT = 'Performs ML modeling and advanced analytics';

-- Data Analyst - BI and reporting
CREATE ROLE IF NOT EXISTS DATA_ANALYST
    COMMENT = 'Consumes data for BI dashboards and ad-hoc analysis';

-- Data Steward - business metadata and definitions
CREATE ROLE IF NOT EXISTS DATA_STEWARD
    COMMENT = 'Manages business metadata, definitions, and data ownership';

-- ============================================================================
-- SECTION 5: ROLE HIERARCHY
-- ============================================================================

-- Grant roles to SYSADMIN for management
GRANT ROLE GOVERNANCE_ADMIN TO ROLE SYSADMIN;
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE DATA_SCIENTIST TO ROLE SYSADMIN;
GRANT ROLE DATA_ANALYST TO ROLE SYSADMIN;
GRANT ROLE DATA_STEWARD TO ROLE SYSADMIN;

-- Data Engineer can also do analyst work
GRANT ROLE DATA_ANALYST TO ROLE DATA_ENGINEER;

-- Data Scientist has analyst capabilities
GRANT ROLE DATA_ANALYST TO ROLE DATA_SCIENTIST;

-- Governance Admin has steward capabilities
GRANT ROLE DATA_STEWARD TO ROLE GOVERNANCE_ADMIN;

-- ============================================================================
-- SECTION 6: WAREHOUSE GRANTS
-- ============================================================================
use role sysadmin;

-- Admin warehouse access
GRANT USAGE ON WAREHOUSE INSURANCECO_ADMIN_WH TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON WAREHOUSE INSURANCECO_ADMIN_WH TO ROLE DATA_STEWARD;

-- ETL warehouse access
GRANT USAGE ON WAREHOUSE INSURANCECO_ETL_WH TO ROLE DATA_ENGINEER;
GRANT OPERATE ON WAREHOUSE INSURANCECO_ETL_WH TO ROLE DATA_ENGINEER;

-- Analytics warehouse access
GRANT USAGE ON WAREHOUSE INSURANCECO_ANALYTICS_WH TO ROLE DATA_ANALYST;
GRANT USAGE ON WAREHOUSE INSURANCECO_ANALYTICS_WH TO ROLE DATA_STEWARD;
GRANT USAGE ON WAREHOUSE INSURANCECO_ANALYTICS_WH TO ROLE DATA_ENGINEER;

-- ML warehouse access
GRANT USAGE ON WAREHOUSE INSURANCECO_ML_WH TO ROLE DATA_SCIENTIST;
GRANT OPERATE ON WAREHOUSE INSURANCECO_ML_WH TO ROLE DATA_SCIENTIST;

-- ============================================================================
-- SECTION 7: DATABASE AND SCHEMA GRANTS
-- ============================================================================

-- Database level grants
GRANT USAGE ON DATABASE INSURANCECO TO ROLE GOVERNANCE_ADMIN;
GRANT USAGE ON DATABASE INSURANCECO TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE INSURANCECO TO ROLE DATA_SCIENTIST;
GRANT USAGE ON DATABASE INSURANCECO TO ROLE DATA_ANALYST;
GRANT USAGE ON DATABASE INSURANCECO TO ROLE DATA_STEWARD;

-- RAW schema - Data Engineers own this
GRANT USAGE ON SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;
GRANT CREATE STAGE ON SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;
GRANT CREATE FILE FORMAT ON SCHEMA INSURANCECO.RAW TO ROLE DATA_ENGINEER;

-- CURATED schema - Data Engineers build, others consume
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_SCIENTIST;
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_STEWARD;
GRANT USAGE ON SCHEMA INSURANCECO.CURATED TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE TABLE ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.CURATED TO ROLE DATA_ENGINEER;

-- ANALYTICS schema - aggregated views
GRANT USAGE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_SCIENTIST;
GRANT USAGE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_STEWARD;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA INSURANCECO.ANALYTICS TO ROLE DATA_ENGINEER;

-- DATA_SCIENCE schema - ML artifacts
GRANT USAGE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_SCIENTIST;
GRANT USAGE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;
GRANT CREATE TABLE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_SCIENTIST;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_SCIENTIST;
GRANT CREATE FUNCTION ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_SCIENTIST;
GRANT CREATE PROCEDURE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_SCIENTIST;
GRANT CREATE TABLE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;
GRANT CREATE FUNCTION ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;
GRANT CREATE PROCEDURE ON SCHEMA INSURANCECO.DATA_SCIENCE TO ROLE DATA_ENGINEER;

-- GOVERNANCE schema - governance objects
GRANT USAGE ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE TAG ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE MASKING POLICY ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE ROW ACCESS POLICY ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE FUNCTION ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE DATA METRIC FUNCTION ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE GOVERNANCE_ADMIN;
GRANT CREATE VIEW ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE DATA_ENGINEER;
GRANT CREATE ALERT ON SCHEMA INSURANCECO.GOVERNANCE TO ROLE DATA_ENGINEER;



use role accountadmin;

-- Allow Governance Admin to apply tags and policies to all schemas
GRANT APPLY TAG ON ACCOUNT TO ROLE GOVERNANCE_ADMIN;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE GOVERNANCE_ADMIN;

-- ============================================================================
-- SECTION 8: GRANT ROLES TO CURRENT USER (for demo purposes)
-- ============================================================================

-- Grant all demo roles to current user for demonstration
-- Replace <YOUR_USERNAME> with actual username or use CURRENT_USER()
GRANT ROLE GOVERNANCE_ADMIN TO USER  skaushal;
GRANT ROLE DATA_ENGINEER TO USER  skaushal;
GRANT ROLE DATA_SCIENTIST TO USER  skaushal;
GRANT ROLE DATA_ANALYST TO USER  skaushal;
GRANT ROLE DATA_STEWARD TO USER  skaushal;

-- ============================================================================
-- SECTION 9: VERIFICATION
-- ============================================================================

-- Verify setup
SHOW WAREHOUSES LIKE 'INSURANCECO%';
SHOW SCHEMAS IN DATABASE INSURANCECO;

SHOW ROLES LIKE '%ADMIN%' OR LIKE '%ENGINEER%' OR LIKE '%SCIENTIST%' OR LIKE '%ANALYST%' OR LIKE '%STEWARD%';

-- Display completion message
SELECT 'Environment setup complete!' AS STATUS,
       'Database: INSURANCECO' AS DATABASE_CREATED,
       '5 Schemas created' AS SCHEMAS,
       '4 Warehouses created' AS WAREHOUSES,
       '5 Custom roles created' AS ROLES;
