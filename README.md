# InsuranceCo Snowflake Horizon Governance Demo

## 🎯 Overview

This demo showcases how **Snowflake Horizon** provides a unified platform for establishing robust data governance, ensuring data quality, and building trust across technical teams at an insurance company.

### Business Problem
- Lack of formal data governance process leading to "breaking of trust" in data
- Reliance on slow, manual processes to generate and validate data
- Compliance risks under GDPR
- Blocked analytics initiatives (fraud detection, customer retention)

### Solution Highlights
- **Horizon Catalog**: Unified data discovery and metadata management
- **Data Metric Functions (DMFs)**: Automated data quality monitoring
- **Tag-Based Masking**: GDPR-compliant sensitive data protection
- **Column-Level Lineage**: End-to-end data flow visualization

---

## 📁 Project Structure

```
gf-forsikring/
├── README.md                          # This file
├── data/
│   ├── raw_claims.csv                 # Sample claims data
│   └── sample_policies.csv            # Sample policy data
├── sql/
│   ├── 01_setup_environment.sql       # Database, schemas, roles, warehouses
│   ├── 02_load_raw_data.sql           # Load sample data into raw tables
│   ├── 03_create_curated_tables.sql   # Transform raw → curated with documentation
│   ├── 04_horizon_catalog_setup.sql   # Catalog configuration & ownership
│   ├── 05_data_metric_functions.sql   # DMFs for quality monitoring
│   ├── 06_tagging_and_masking.sql     # PII tags & masking policies
│   ├── 07_lineage_demo.sql            # Views & transformations for lineage
│   └── 08_jira_integration.sql        # Alert handler for JIRA tickets
├── notebooks/
│   └── fraud_detection_model.ipynb    # Snowpark ML notebook
└── DEMO_WALKTHROUGH.md                # Step-by-step demo script
```

---

## 🚀 Quick Start

### Prerequisites
- Snowflake Enterprise Edition (or higher) account
- ACCOUNTADMIN role access for initial setup
- Network rule configured for JIRA integration (optional)

### Setup Instructions

1. **Execute SQL scripts in order:**
   ```sql
   -- Run each script in Snowsight
   -- 01 → 02 → 03 → 04 → 05 → 06 → 07 → 08
   ```

2. **Upload sample data:**
   - Upload `data/raw_claims.csv` to stage `@INSURANCECO.RAW.CLAIMS_STAGE`
   - Upload `data/sample_policies.csv` to stage `@INSURANCECO.RAW.POLICIES_STAGE`

3. **Import notebook:**
   - Import `notebooks/fraud_detection_model.ipynb` into Snowsight

---

## 🎬 Demo Vignettes

### Vignette 1: Establishing a Single Source of Truth
**Audience:** Data Steward, Data Analyst

| Feature | What to Show |
|---------|--------------|
| Horizon Catalog | Search for "claims", discover tables |
| Business Definitions | Column comments, table descriptions |
| Data Stewardship | Ownership assignment, @mentions |

### Vignette 2: Automating Data Trust with Quality & Lineage
**Audience:** Data Engineer, Data Steward

| Feature | What to Show |
|---------|--------------|
| System Metrics | Freshness, volume monitoring |
| Custom DMFs | Business rule validation |
| Column Lineage | Visual data flow tracing |

### Vignette 3: Secure AI & Compliance at Scale
**Audience:** Data Scientist, Data Engineer

| Feature | What to Show |
|---------|--------------|
| PII Tagging | Classify sensitive columns |
| Dynamic Masking | Role-based data access |
| Snowpark ML | Fraud detection model training |
| JIRA Integration | Automated alert tickets |

---

## 👥 Roles Used in Demo

| Role | Purpose |
|------|---------|
| `ACCOUNTADMIN` | Initial setup only |
| `GOVERNANCE_ADMIN` | Manage tags, policies, stewardship |
| `DATA_ENGINEER` | Build pipelines, transformations |
| `DATA_SCIENTIST` | Full access for ML workloads |
| `DATA_ANALYST` | Masked access for reporting |

---

## 📊 Sample Data Schema

### RAW_CLAIMS
| Column | Type | Description |
|--------|------|-------------|
| claim_id | VARCHAR | Unique claim identifier |
| policy_id | VARCHAR | Reference to policy |
| claim_amount | NUMBER | Claimed amount in DKK |
| date_of_incident | DATE | When incident occurred |
| date_reported | DATE | When claim was filed |
| claim_type | VARCHAR | Type (collision, theft, etc.) |
| claim_status | VARCHAR | Current status |
| policy_holder_name | VARCHAR | Customer name (PII) |
| policy_holder_email | VARCHAR | Customer email (PII) |
| policy_holder_cpr | VARCHAR | Danish CPR number (PII) |
| address | VARCHAR | Customer address (PII) |
| fraud_flag | BOOLEAN | Known fraud indicator |

---

## 🔗 Key Snowflake Features Demonstrated

- **Horizon Catalog**: Native data discovery and documentation
- **Data Quality Monitoring**: System metrics + custom DMFs
- **Object Lineage**: Automatic column-level tracking
- **Tags & Policies**: Scalable classification and masking
- **Snowpark for Python**: Secure ML within governance boundary
- **Tasks & Alerts**: Automated operational workflows

---

## 📝 License

This demo is for demonstration purposes only. Sample data is synthetic and does not represent real customer information.

---

*Built for GF Forsikring Snowflake Demo*
