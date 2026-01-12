# 🎬 InsuranceCo Snowflake Horizon Demo Walkthrough

## Complete Demo Script for GF Forsikring

**Duration:** 45-60 minutes  
**Audience:** Data Engineers, Data Analysts, Data Scientists, Data Stewards  
**Key Theme:** Building Trust in Data Through Governance

---

## 🎯 Opening Hook (2 minutes)

> "Today, your team spends significant time manually validating data before they can trust it for insights. Every time a report looks wrong, it triggers a fire drill to find the root cause. And with GDPR, you're constantly worried about who has access to customer PII.
> 
> What if I told you that Snowflake can automatically:
> - Tell you when your data is stale or incorrect
> - Show you exactly where any piece of data came from
> - Mask sensitive data based on who's looking at it
> - All without any separate tools to maintain?"

---

## 📋 Pre-Demo Checklist

Before starting, ensure:

- [ ] All SQL scripts (01-08) have been executed successfully
- [ ] Sample data is loaded in `INSURANCECO.RAW` schema
- [ ] Curated tables exist in `INSURANCECO.CURATED` schema
- [ ] Tags and masking policies are applied
- [ ] You have access to all demo roles (DATA_ANALYST, DATA_SCIENTIST, etc.)
- [ ] Snowpark notebook is imported into Snowsight

---

## Vignette 1: Establishing a Single Source of Truth

### Duration: 15 minutes

### 1.1 Set the Stage (2 min)

**Say:**
> "Let's start with a common problem: Your new data analyst just joined the team. They need to find claims data for a report. Where do they start?"

**Do:**
1. Navigate to **Data > Databases > INSURANCECO > RAW**
2. Click on `RAW_CLAIMS` table

**Say:**
> "This is what they find. Raw data with no documentation. What does `policy_holder_cpr` mean? Is this table trustworthy? They have no idea."

### 1.2 Discover Data with Horizon Catalog (5 min)

**Say:**
> "Now let's see how Snowflake Horizon changes this experience."

**Do:**
1. Click the **Search** icon (magnifying glass) in Snowsight top bar
2. Type: `claims`
3. Show the search results

**Say:**
> "Notice how the search surfaces multiple results - both RAW_CLAIMS and DIM_CLAIMS. Look at the quality indicators."

**Do:**
1. Click on `DIM_CLAIMS` in the search results
2. Show the table detail page

**Highlight:**
- Table description at the top
- Data domain tag: `CLAIMS`
- Quality tier: `GOLD`
- Data steward contact

**Say:**
> "This is the curated version. It has a clear description, an assigned steward, and quality certification. Your analyst knows this is the table to use."

### 1.3 Explore Column Documentation (3 min)

**Do:**
1. Scroll down to see column list
2. Hover over `POLICY_HOLDER_CPR`

**Say:**
> "Every column has documentation. CPR is the Danish equivalent of a Social Security Number. And look - it's tagged as SENSITIVE PII with GDPR classification. This metadata drives automatic protection."

**Do:**
1. Click on `CLAIM_AMOUNT` column
2. Show the column detail including:
   - Comment: "Amount claimed in Danish Kroner (DKK)"
   - Semantic category: MEASURE
   - Tags applied

### 1.4 Quick Query from Catalog (2 min)

**Say:**
> "The best part? From discovery to analysis is one click."

**Do:**
1. Click **Query Data** button (or the query icon)
2. Show the worksheet that opens with `DIM_CLAIMS` pre-selected
3. Run: `SELECT * FROM DIM_CLAIMS LIMIT 10`

**🎯 WOW Moment:**
> "Discovery and analysis in the same tool. No context switching, no searching through documentation wikis. The catalog IS the documentation."

### 1.5 Reinforce Value (3 min)

**Say:**
> "Think about what we just did:
> - Found the right table in seconds
> - Understood what every column means
> - Knew who to contact with questions
> - Started analyzing immediately
> 
> How long does this take today? Hours? Days? With Horizon, it's minutes."

---

## Vignette 2: Automating Data Trust with Quality & Lineage

### Duration: 15 minutes

### 2.1 Set the Stage (2 min)

**Say:**
> "Now that we can find data, the next question is: Can we trust it? When a dashboard shows a number that looks wrong, how quickly can you find out why?"

**Do:**
1. Stay on `DIM_CLAIMS` table detail page
2. Click the **Data Quality** tab (or navigate to quality monitoring)

### 2.2 Show System Data Metrics (4 min)

**Say:**
> "Snowflake automatically monitors data quality. Let's see what it's tracking."

**Do:**
1. Show the built-in metrics:
   - **Freshness**: When was this data last updated?
   - **Row Count**: Is volume consistent?
   - **NULL counts**: Are required fields complete?

**Say:**
> "These system metrics run automatically. No configuration needed. Snowflake tracks freshness, volume changes, and completeness out of the box."

### 2.3 Show Custom Data Metric Functions (5 min)

**Say:**
> "But the real power is custom business rules. Let's see the DMFs we created."

**Do:**
1. Run the following query in a worksheet:

```sql
SELECT 
    metric_name,
    value,
    measurement_time
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_name = 'DIM_CLAIMS'
ORDER BY measurement_time DESC
LIMIT 10;
```

**Highlight the key DMFs:**

| DMF | What It Checks | Current Value |
|-----|---------------|---------------|
| `DMF_CLAIMS_EXCEEDING_COVERAGE` | Claims > policy limit | 5 |
| `DMF_FRAUD_FLAG_RATE` | % flagged for fraud | 25% |
| `DMF_SAME_DAY_CLAIMS` | Reported same day | 4 |

**Say:**
> "Look at this - we have 5 claims where the amount exceeds the policy coverage limit. That's either a data error or potential fraud. The DMF caught this automatically."

**Do:**
1. Run query to show the violating records:

```sql
SELECT 
    claim_id,
    claim_amount,
    policy_coverage_limit,
    claim_amount - policy_coverage_limit AS excess_amount,
    adjuster_notes
FROM INSURANCECO.CURATED.DIM_CLAIMS
WHERE claim_amount > policy_coverage_limit;
```

**🎯 WOW Moment:**
> "These DMFs are just SQL. Your team already knows how to write them. And they run automatically - every time the data changes."

### 2.4 Demonstrate Column-Level Lineage (4 min)

**Say:**
> "When a quality issue is found, the next question is: where did this data come from?"

**Do:**
1. Navigate to `DIM_CLAIMS` in the catalog
2. Click the **Lineage** tab
3. Show the lineage graph

**Say:**
> "This is automatic lineage. Snowflake tracks every transformation. Let's trace a specific column."

**Do:**
1. Click on `CLAIM_AMOUNT` column in the lineage view
2. Show the highlighted path: `RAW_CLAIMS.claim_amount` → `DIM_CLAIMS.CLAIM_AMOUNT`

**Say:**
> "Now let's look downstream. What reports depend on this data?"

**Do:**
1. Show downstream dependencies:
   - `V_CLAIMS_BY_REGION`
   - `V_HIGH_RISK_CLAIMS`
   - `AGG_CLAIMS_EXECUTIVE`
   - `FRAUD_DETECTION_FEATURES`

**🎯 WOW Moment:**
> "If there's a data quality issue in DIM_CLAIMS, I can instantly see every downstream report and model that's affected. Impact analysis in seconds, not hours."

---

## Vignette 3: Secure AI & Compliance at Scale

### Duration: 20 minutes

### 3.1 Set the Stage (2 min)

**Say:**
> "Your data scientists want to build a fraud detection model. They need access to claims data, including customer information for pattern analysis. But GDPR requires you to protect that PII. How do you enable innovation while ensuring compliance?"

### 3.2 Show PII Tags (3 min)

**Do:**
1. Switch to `GOVERNANCE_ADMIN` role:

```sql
USE ROLE GOVERNANCE_ADMIN;
```

2. Show tags on DIM_CLAIMS:

```sql
SELECT 
    column_name,
    tag_name,
    tag_value
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'INSURANCECO.CURATED.DIM_CLAIMS', 'TABLE'
))
WHERE tag_name = 'PII'
ORDER BY column_name;
```

**Say:**
> "We've classified 4 columns as PII: name, email, CPR number, and address. Each has a specific classification that drives masking."

### 3.3 Demonstrate Dynamic Masking - The Magic Moment (8 min)

**Say:**
> "Now watch this. I'm going to run the exact same query with two different roles."

**Do:**
1. First, run as DATA_ANALYST:

```sql
USE ROLE DATA_ANALYST;
USE WAREHOUSE INSURANCECO_ANALYTICS_WH;

SELECT 
    claim_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    claim_amount,
    fraud_flag
FROM INSURANCECO.CURATED.DIM_CLAIMS
LIMIT 5;
```

**Show result - all PII is masked:**
| claim_id | policy_holder_name | policy_holder_email | policy_holder_cpr | address |
|----------|-------------------|--------------------|--------------------|---------|
| CLM-2025-00001 | ***MASKED*** | ****@****.*** | ******-**** | ***MASKED ADDRESS*** |

**Say:**
> "As a Data Analyst, I see claim amounts and fraud flags - everything I need for reporting. But PII is fully masked."

**Do:**
2. Now run the EXACT same query as DATA_SCIENTIST:

```sql
USE ROLE DATA_SCIENTIST;
USE WAREHOUSE INSURANCECO_ML_WH;

SELECT 
    claim_id,
    policy_holder_name,
    policy_holder_email,
    policy_holder_cpr,
    address,
    claim_amount,
    fraud_flag
FROM INSURANCECO.CURATED.DIM_CLAIMS
LIMIT 5;
```

**Show result - full data visible:**
| claim_id | policy_holder_name | policy_holder_email | policy_holder_cpr | address |
|----------|-------------------|--------------------|--------------------|---------|
| CLM-2025-00001 | Anders Jensen | anders.jensen@email.dk | 010185-1234 | Vestergade 42 |

**🎯 WOW Moment - KEY DEMO MOMENT:**
> "SAME query. SAME table. Different results based on role. The Data Scientist sees real data for modeling. The Analyst sees masked data for compliance. No separate data copies. No manual provisioning. The policy enforces itself."

**Say:**
> "This is tag-based masking. We define the policy once, attach it to a tag, and every column with that tag is automatically protected. Add 100 new PII columns tomorrow? Just tag them - protection is instant."

### 3.4 Show Snowpark ML Integration (5 min)

**Say:**
> "Now let's see how the Data Scientist uses this governed data for machine learning."

**Do:**
1. Open the Snowpark notebook `fraud_detection_model.ipynb`
2. Show key cells (don't run all - just highlight):

**Cell 1: Connection**
```python
session = get_active_session()
print(f"Role: {session.get_current_role()}")  # DATA_SCIENTIST
```

**Cell 2: Load Data**
```python
claims_df = session.table("INSURANCECO.CURATED.DIM_CLAIMS")
```

**Say:**
> "The data scientist works directly with governed data. Everything stays in Snowflake."

**Cell 3: Show PII Access**
```python
claims_df.select("CLAIM_ID", "POLICY_HOLDER_NAME", "CLAIM_AMOUNT").show()
```

**Say:**
> "As DATA_SCIENTIST, they see full PII because they need it for fraud pattern analysis. But the access is logged for compliance."

**Cell 4: Train Model (briefly show)**
```python
rf_model = RandomForestClassifier(...)
rf_model.fit(train_df)  # Training happens IN Snowflake
```

**Say:**
> "Model training happens inside Snowflake. Data never leaves the governance boundary. No exports to laptops. No compliance risk."

### 3.5 Show JIRA Integration (2 min)

**Say:**
> "Finally, when data quality issues are detected, we can automatically notify the right people."

**Do:**
1. Run the demo alert:

```sql
USE ROLE GOVERNANCE_ADMIN;
CALL INSURANCECO.GOVERNANCE.TRIGGER_QUALITY_ALERT_DEMO(
    'High value claim detected - requires senior adjuster review',
    'High'
);
```

**Show the response:**

```json
{
  "demo_mode": true,
  "ticket": {
    "key": "DQ-1234",
    "summary": "DEMO ALERT: High value claim detected...",
    "priority": "High",
    "status": "To Do"
  }
}
```

**Say:**
> "In production, this creates a real JIRA ticket. No manual monitoring. The system watches itself."

---

## 🎬 Closing Summary (3 minutes)

**Do:**
Pull up this summary slide/table:

| Before | After (with Horizon) |
|--------|---------------------|
| Manual data search | Instant catalog discovery |
| Undocumented tables | Rich business metadata |
| Manual quality checks | Automated DMF monitoring |
| Hours to find root cause | Column-level lineage in seconds |
| Manual PII provisioning | Tag-based dynamic masking |
| Compliance risk | Automated policy enforcement |
| Data science silos | Governed ML in Snowpark |

**Say:**
> "Let me summarize what we demonstrated today:
>
> 1. **Single Source of Truth** - Your team can find, understand, and trust data in seconds with the Horizon Catalog
>
> 2. **Automated Data Quality** - DMFs continuously monitor your data and alert you to issues before they reach business reports
>
> 3. **Complete Lineage** - When something goes wrong, you trace it back to source in seconds, not days
>
> 4. **GDPR Compliance at Scale** - Tag-based masking means same data, different views based on role. No manual provisioning.
>
> 5. **Secure AI** - Data scientists work on real, governed data without creating compliance risks
>
> The key message: **All of this is built into Snowflake. No separate tools to maintain. No integration to break. Your governance is always in sync because it lives with your data.**"

---

## 💬 Common Questions & Answers

**Q: How much does this cost?**
> Data Quality Monitoring (DMFs) is included in Enterprise edition. Tag-based masking is included. Lineage is automatic. The only incremental cost is the compute for running DMFs.

**Q: How long to implement?**
> You can start with basic catalog and tagging in days. DMFs can be built incrementally. Full implementation typically 4-8 weeks depending on complexity.

**Q: What if we have existing governance tools?**
> Horizon doesn't replace your entire stack - it provides native capabilities where data lives. Many customers use Horizon for enforcement while keeping their broader catalog tools for cross-platform views.

**Q: How does this handle real-time data?**
> DMFs can run on triggers (when data changes) or on schedule. Masking applies in real-time to every query. Lineage is captured as queries execute.

---

## 📦 Demo Reset Instructions

To reset the demo for another run:

```sql
-- Truncate and reload data
TRUNCATE TABLE INSURANCECO.RAW.RAW_CLAIMS;
TRUNCATE TABLE INSURANCECO.RAW.RAW_POLICIES;

-- Re-run scripts 02, 03 to reload data
-- Scripts 04-08 don't need re-running (governance objects persist)
```

---

*Demo Script Version 1.0 - January 2025*
*Built for GF Forsikring Snowflake Horizon Demonstration*
