/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 08: JIRA Integration for Automated Alerts
================================================================================
Purpose: Create alert handlers that automatically create JIRA tickets when 
         data quality issues are detected
Author: Demo Setup Script
Date: 2025-01

VIGNETTE 3 (Enhancement): Operational Integration
================================================================================
*/

-- ============================================================================
-- SECTION 1: SET CONTEXT
-- ============================================================================
USE ROLE ACCOUNTADMIN;  -- Required for network and secret setup
USE WAREHOUSE INSURANCECO_ADMIN_WH;
USE DATABASE INSURANCECO;
USE SCHEMA GOVERNANCE;

-- ============================================================================
-- SECTION 2: CREATE NETWORK RULE FOR JIRA API
-- ============================================================================

/*
 * Network rules define which external endpoints Snowflake can communicate with.
 * This is required for calling external APIs like JIRA.
 */

-- Create network rule for JIRA Cloud API
CREATE OR REPLACE NETWORK RULE JIRA_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('your-company.atlassian.net:443')  -- Replace with actual JIRA domain
    COMMENT = 'Allow outbound connections to JIRA Cloud API';

-- ============================================================================
-- SECTION 3: CREATE SECRET FOR JIRA API TOKEN
-- ============================================================================

/*
 * Secrets securely store credentials for external services.
 * The API token is never exposed in query logs or results.
 */

-- Create secret for JIRA API authentication
-- Note: Replace with actual API token in production
CREATE OR REPLACE SECRET JIRA_API_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'your-jira-api-token-here'  -- Replace with actual token
    COMMENT = 'JIRA API token for automated ticket creation';

-- ============================================================================
-- SECTION 4: CREATE EXTERNAL ACCESS INTEGRATION
-- ============================================================================

/*
 * External access integrations combine network rules and secrets
 * to enable secure external API calls from Snowflake.
 */

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION JIRA_INTEGRATION
    ALLOWED_NETWORK_RULES = (JIRA_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (JIRA_API_SECRET)
    ENABLED = TRUE
    COMMENT = 'Integration for creating JIRA tickets from Snowflake alerts';

-- ============================================================================
-- SECTION 5: CREATE JIRA TICKET CREATION FUNCTION
-- ============================================================================

USE ROLE GOVERNANCE_ADMIN;
USE SCHEMA INSURANCECO.GOVERNANCE;

/*
 * This stored procedure creates JIRA tickets via the JIRA REST API.
 * It uses Snowpark Python for HTTP calls.
 */

CREATE OR REPLACE PROCEDURE CREATE_JIRA_TICKET(
    SUMMARY VARCHAR,
    DESCRIPTION VARCHAR,
    ISSUE_TYPE VARCHAR DEFAULT 'Bug',
    PRIORITY VARCHAR DEFAULT 'Medium',
    ASSIGNEE VARCHAR DEFAULT NULL,
    LABELS ARRAY DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'create_ticket'
EXTERNAL_ACCESS_INTEGRATIONS = (JIRA_INTEGRATION)
SECRETS = ('jira_token' = JIRA_API_SECRET)
COMMENT = 'Creates a JIRA ticket for data quality alerts'
AS
$$
import _snowflake
import requests
import json

def create_ticket(session, summary, description, issue_type='Bug', priority='Medium', assignee=None, labels=None):
    """
    Create a JIRA ticket via REST API.
    
    In demo mode, this simulates the API call and returns mock response.
    For production, uncomment the actual API call section.
    """
    
    # Configuration - Replace with actual values
    JIRA_BASE_URL = "https://your-company.atlassian.net"
    JIRA_PROJECT_KEY = "DQ"  # Data Quality project
    JIRA_USER_EMAIL = "data-quality@insuranceco.dk"
    
    # Get API token from secret
    api_token = _snowflake.get_generic_secret_string('jira_token')
    
    # Build ticket payload
    ticket_payload = {
        "fields": {
            "project": {"key": JIRA_PROJECT_KEY},
            "summary": summary,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": description}]
                    }
                ]
            },
            "issuetype": {"name": issue_type},
            "priority": {"name": priority}
        }
    }
    
    # Add optional fields
    if assignee:
        ticket_payload["fields"]["assignee"] = {"accountId": assignee}
    
    if labels:
        ticket_payload["fields"]["labels"] = labels
    
    # === DEMO MODE: Simulate API call ===
    # In production, uncomment the actual API call below
    
    mock_response = {
        "demo_mode": True,
        "message": "JIRA ticket would be created with following details:",
        "ticket": {
            "key": f"{JIRA_PROJECT_KEY}-{hash(summary) % 10000}",
            "summary": summary,
            "description": description[:200] + "..." if len(description) > 200 else description,
            "issue_type": issue_type,
            "priority": priority,
            "status": "To Do",
            "created": "2025-01-12T10:00:00.000Z"
        },
        "api_endpoint": f"{JIRA_BASE_URL}/rest/api/3/issue"
    }
    
    return mock_response
    
    # === PRODUCTION MODE: Actual API call ===
    # Uncomment below for production use
    
    # try:
    #     response = requests.post(
    #         f"{JIRA_BASE_URL}/rest/api/3/issue",
    #         json=ticket_payload,
    #         auth=(JIRA_USER_EMAIL, api_token),
    #         headers={"Content-Type": "application/json"}
    #     )
    #     response.raise_for_status()
    #     return response.json()
    # except Exception as e:
    #     return {"error": str(e), "payload": ticket_payload}
$$;

-- ============================================================================
-- SECTION 6: CREATE DATA QUALITY ALERT HANDLER
-- ============================================================================

/*
 * This procedure checks DMF results and creates JIRA tickets
 * for any quality issues that exceed thresholds.
 */

CREATE OR REPLACE PROCEDURE HANDLE_DATA_QUALITY_ALERTS()
RETURNS TABLE(
    alert_type VARCHAR,
    metric_name VARCHAR,
    metric_value NUMBER,
    threshold NUMBER,
    jira_response VARIANT
)
LANGUAGE SQL
COMMENT = 'Checks DMF results and creates JIRA tickets for quality violations'
AS
$$
DECLARE
    result_set RESULTSET;
    alert_type VARCHAR;
    metric_name VARCHAR;
    metric_value NUMBER;
    jira_response VARIANT;
BEGIN
    -- Create temp table to store alerts
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ALERTS (
        alert_type VARCHAR,
        metric_name VARCHAR,
        metric_value NUMBER,
        threshold NUMBER,
        jira_response VARIANT
    );
    
    -- Check for claims exceeding coverage (threshold: 0)
    SELECT value INTO :metric_value
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_CLAIMS_EXCEEDING_COVERAGE'
    ORDER BY measurement_time DESC
    LIMIT 1;
    
    IF (metric_value IS NOT NULL AND metric_value > 0) THEN
        CALL CREATE_JIRA_TICKET(
            'DATA QUALITY ALERT: Claims Exceeding Coverage Detected',
            'The DMF_CLAIMS_EXCEEDING_COVERAGE metric detected ' || :metric_value || 
            ' claims where claim_amount exceeds policy_coverage_limit. ' ||
            'This may indicate data errors or potential fraud. Please investigate.',
            'Bug',
            'High',
            NULL,
            ARRAY_CONSTRUCT('data-quality', 'claims', 'automated')
        ) INTO :jira_response;
        
        INSERT INTO TEMP_ALERTS VALUES (
            'CRITICAL', 
            'DMF_CLAIMS_EXCEEDING_COVERAGE', 
            :metric_value, 
            0,
            :jira_response
        );
    END IF;
    
    -- Check for high fraud rate (threshold: 20%)
    SELECT value INTO :metric_value
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_name = 'DIM_CLAIMS'
      AND metric_name = 'DMF_FRAUD_FLAG_RATE'
    ORDER BY measurement_time DESC
    LIMIT 1;
    
    IF (metric_value IS NOT NULL AND metric_value > 20) THEN
        CALL CREATE_JIRA_TICKET(
            'DATA QUALITY ALERT: High Fraud Flag Rate',
            'The fraud flag rate has exceeded 20% threshold. Current rate: ' || :metric_value || 
            '%. This unusual pattern requires immediate attention from the fraud investigation team.',
            'Task',
            'High',
            NULL,
            ARRAY_CONSTRUCT('data-quality', 'fraud', 'automated')
        ) INTO :jira_response;
        
        INSERT INTO TEMP_ALERTS VALUES (
            'WARNING', 
            'DMF_FRAUD_FLAG_RATE', 
            :metric_value, 
            20,
            :jira_response
        );
    END IF;
    
    -- Check for duplicate claim IDs (threshold: 0)
    SELECT value INTO :metric_value
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_name = 'DIM_CLAIMS'
      AND metric_name = 'DUPLICATE_COUNT'
      AND column_name = 'CLAIM_ID'
    ORDER BY measurement_time DESC
    LIMIT 1;
    
    IF (metric_value IS NOT NULL AND metric_value > 0) THEN
        CALL CREATE_JIRA_TICKET(
            'DATA QUALITY ALERT: Duplicate Claim IDs Detected',
            'Found ' || :metric_value || ' duplicate CLAIM_ID values in DIM_CLAIMS. ' ||
            'This violates data integrity rules. Data Engineering team must investigate ETL pipeline.',
            'Bug',
            'Critical',
            NULL,
            ARRAY_CONSTRUCT('data-quality', 'duplicates', 'etl', 'automated')
        ) INTO :jira_response;
        
        INSERT INTO TEMP_ALERTS VALUES (
            'CRITICAL', 
            'DUPLICATE_COUNT', 
            :metric_value, 
            0,
            :jira_response
        );
    END IF;
    
    -- Return all alerts
    result_set := (SELECT * FROM TEMP_ALERTS);
    RETURN TABLE(result_set);
END;
$$;

-- ============================================================================
-- SECTION 7: CREATE SCHEDULED TASK FOR ALERTS
-- ============================================================================

/*
 * This task runs the alert handler periodically to check for quality issues.
 */

CREATE OR REPLACE TASK TASK_DATA_QUALITY_ALERTS
    WAREHOUSE = INSURANCECO_ADMIN_WH
    SCHEDULE = 'USING CRON 0 */4 * * * Europe/Copenhagen'  -- Every 4 hours
    COMMENT = 'Scheduled task to check DMF results and create JIRA tickets for violations'
AS
    CALL HANDLE_DATA_QUALITY_ALERTS();

-- Note: Task needs to be resumed to start running
-- ALTER TASK TASK_DATA_QUALITY_ALERTS RESUME;

-- ============================================================================
-- SECTION 8: CREATE MANUAL ALERT TRIGGER
-- ============================================================================

/*
 * Procedure to manually trigger alert check (useful for demos)
 */

CREATE OR REPLACE PROCEDURE TRIGGER_QUALITY_ALERT_DEMO(
    ALERT_MESSAGE VARCHAR,
    SEVERITY VARCHAR DEFAULT 'Medium'
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Manually trigger a demo JIRA alert for demonstration purposes'
AS
$$
DECLARE
    jira_response VARIANT;
BEGIN
    CALL CREATE_JIRA_TICKET(
        'DEMO ALERT: ' || :ALERT_MESSAGE,
        'This is a demonstration alert triggered manually. ' ||
        'In production, these alerts are generated automatically by Data Metric Functions. ' ||
        'Alert Details: ' || :ALERT_MESSAGE,
        'Task',
        :SEVERITY,
        NULL,
        ARRAY_CONSTRUCT('demo', 'data-quality', 'manual')
    ) INTO :jira_response;
    
    RETURN jira_response;
END;
$$;

-- ============================================================================
-- SECTION 9: CREATE ALERT HISTORY TABLE
-- ============================================================================

/*
 * Track all alerts for audit and reporting purposes
 */

CREATE OR REPLACE TABLE ALERT_HISTORY (
    alert_id NUMBER AUTOINCREMENT PRIMARY KEY,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    alert_type VARCHAR(50),
    metric_name VARCHAR(100),
    metric_value NUMBER,
    threshold_value NUMBER,
    severity VARCHAR(20),
    jira_ticket_key VARCHAR(50),
    jira_response VARIANT,
    resolved_at TIMESTAMP_NTZ,
    resolved_by VARCHAR(100),
    resolution_notes VARCHAR(1000)
)
COMMENT = 'History of all data quality alerts and their resolutions';

-- ============================================================================
-- SECTION 10: DEMO SCRIPT - JIRA INTEGRATION
-- ============================================================================

/*
DEMO WALKTHROUGH - JIRA Integration

1. EXPLAIN THE USE CASE
   - "When DMFs detect quality issues, we need to notify the right people"
   - "Manual monitoring doesn't scale - we need automation"
   - "Integration with existing tools (JIRA) keeps teams in their workflow"

2. SHOW THE ARCHITECTURE
   - Network Rule → allows connection to JIRA
   - Secret → stores API credentials securely
   - External Access Integration → combines them
   - Stored Procedure → makes the API call
   - Task → schedules automatic checks

3. TRIGGER A DEMO ALERT
   
   -- Run this to create a demo JIRA ticket
   CALL TRIGGER_QUALITY_ALERT_DEMO(
       'High value claim detected - requires senior adjuster review',
       'High'
   );
   
   -- Show the response
   -- Point out: "In demo mode, we simulate the API call"
   -- Point out: "In production, this creates a real JIRA ticket"

4. SHOW THE AUTOMATED FLOW
   
   -- Run the full alert handler
   CALL HANDLE_DATA_QUALITY_ALERTS();
   
   -- Point out: "This checks all DMF results against thresholds"
   -- Point out: "Creates tickets only for violations"
   -- Point out: "Runs automatically every 4 hours via Task"

5. EXPLAIN THE BUSINESS VALUE
   - "Data quality issues → automatic JIRA tickets"
   - "Assigned to the right person (Data Steward)"
   - "Includes all context needed to investigate"
   - "No manual monitoring required"
   - "Audit trail of all alerts and resolutions"

6. CONNECT TO LINEAGE
   - "Remember the lineage graph?"
   - "When a DMF fails, we know exactly which downstream objects are affected"
   - "JIRA ticket includes impact analysis"

KEY TALKING POINTS:
- "This connects Snowflake to your existing operational tools"
- "Credentials are stored securely in Snowflake Secrets"
- "Network rules control exactly which external endpoints can be reached"
- "Everything is audited and governed"

THE WOW MOMENT:
Run TRIGGER_QUALITY_ALERT_DEMO() and show the JIRA ticket payload.
Point out that this exact payload would create a real ticket in production.
Then show the Task that runs this automatically.
*/

-- ============================================================================
-- SECTION 11: VERIFICATION
-- ============================================================================

-- Show integration objects
SHOW NETWORK RULES;
SHOW EXTERNAL ACCESS INTEGRATIONS;
SHOW PROCEDURES LIKE '%JIRA%';
SHOW TASKS LIKE '%QUALITY%';

-- Test the demo alert trigger
CALL TRIGGER_QUALITY_ALERT_DEMO(
    'Test alert for demonstration',
    'Low'
);

SELECT 'JIRA integration setup complete!' AS STATUS,
       'Network rule and integration created' AS NETWORK,
       'Alert procedures created' AS PROCEDURES,
       'Scheduled task created (suspended)' AS TASK,
       'Ready for integration demonstration' AS NEXT_STEP;
