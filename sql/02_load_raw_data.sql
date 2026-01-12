/*
================================================================================
INSURANCECO SNOWFLAKE HORIZON DEMO
Script 02: Load Raw Data
================================================================================
Purpose: Create stages, file formats, and load raw claims/policy data
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
USE SCHEMA RAW;

-- ============================================================================
-- SECTION 2: CREATE FILE FORMATS
-- ============================================================================

-- CSV file format for claims data
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format for insurance data files';

-- ============================================================================
-- SECTION 3: CREATE INTERNAL STAGES
-- ============================================================================

-- Stage for claims data
CREATE OR REPLACE STAGE CLAIMS_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for claims data files';

-- Stage for policy data
CREATE OR REPLACE STAGE POLICIES_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for policy data files';

-- Stage for unstructured data (PDFs, images)
CREATE OR REPLACE STAGE DOCUMENTS_STAGE
    COMMENT = 'Internal stage for unstructured claim documents';

-- ============================================================================
-- SECTION 4: CREATE RAW TABLES
-- ============================================================================

-- Raw claims table - exactly as received from source
CREATE OR REPLACE TABLE RAW_CLAIMS (
    claim_id VARCHAR(20),
    policy_id VARCHAR(20),
    claim_amount NUMBER(12,2),
    policy_coverage_limit NUMBER(12,2),
    date_of_incident DATE,
    date_reported DATE,
    claim_type VARCHAR(50),
    claim_status VARCHAR(50),
    policy_holder_name VARCHAR(200),
    policy_holder_email VARCHAR(200),
    policy_holder_cpr VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    postal_code VARCHAR(10),
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(50),
    vehicle_year NUMBER(4),
    damage_description VARCHAR(1000),
    fraud_flag BOOLEAN,
    adjuster_notes VARCHAR(2000),
    -- Audit columns
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
)
COMMENT = 'Raw claims data - landing zone from source systems. DO NOT USE FOR ANALYTICS.';

-- Raw policies table
CREATE OR REPLACE TABLE RAW_POLICIES (
    policy_id VARCHAR(20),
    policy_holder_name VARCHAR(200),
    policy_holder_email VARCHAR(200),
    policy_holder_cpr VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    postal_code VARCHAR(10),
    policy_type VARCHAR(50),
    coverage_limit NUMBER(12,2),
    premium_annual NUMBER(10,2),
    policy_start_date DATE,
    policy_end_date DATE,
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(50),
    vehicle_year NUMBER(4),
    vehicle_vin VARCHAR(50),
    driver_age NUMBER(3),
    years_licensed NUMBER(3),
    previous_claims_count NUMBER(5),
    risk_score VARCHAR(20),
    -- Audit columns
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
)
COMMENT = 'Raw policy data - landing zone from policy administration system';

-- ============================================================================
-- SECTION 5: LOAD SAMPLE DATA DIRECTLY (For Demo)
-- ============================================================================

-- Insert sample claims data directly (alternative to file upload)
INSERT INTO RAW_CLAIMS (
    claim_id, policy_id, claim_amount, policy_coverage_limit, date_of_incident,
    date_reported, claim_type, claim_status, policy_holder_name, policy_holder_email,
    policy_holder_cpr, address, city, postal_code, vehicle_make, vehicle_model,
    vehicle_year, damage_description, fraud_flag, adjuster_notes
)
VALUES
    ('CLM-2025-00001', 'POL-DK-100234', 45000.00, 500000.00, '2025-01-03', '2025-01-04', 'collision', 'approved', 'Anders Jensen', 'anders.jensen@email.dk', '010185-1234', 'Vestergade 42', 'København', '1456', 'Volvo', 'XC60', 2022, 'Front bumper damage from parking incident', FALSE, 'Standard collision claim. Photos verified.'),
    ('CLM-2025-00002', 'POL-DK-100567', 125000.00, 750000.00, '2025-01-05', '2025-01-05', 'theft', 'under_review', 'Marie Nielsen', 'marie.nielsen@email.dk', '150990-5678', 'Nørrebrogade 118', 'København', '2200', 'BMW', 'X5', 2023, 'Vehicle stolen from underground parking', FALSE, 'Police report #CPH-2025-0892 filed'),
    ('CLM-2025-00003', 'POL-DK-100891', 8500.00, 250000.00, '2025-01-02', '2025-01-06', 'vandalism', 'approved', 'Lars Petersen', 'lars.petersen@email.dk', '220375-9012', 'Hovedgaden 55', 'Aarhus', '8000', 'Toyota', 'Corolla', 2020, 'Keyed door panels and broken mirror', FALSE, 'Neighborhood incident. Multiple cars affected.'),
    ('CLM-2025-00004', 'POL-DK-101234', 350000.00, 300000.00, '2025-01-04', '2025-01-04', 'collision', 'flagged', 'Henrik Madsen', 'henrik.madsen@email.dk', '080292-3456', 'Algade 23', 'Odense', '5000', 'Mercedes', 'E-Class', 2024, 'Total loss - highway accident', TRUE, 'ALERT: Claim exceeds coverage. Multiple recent claims.'),
    ('CLM-2025-00005', 'POL-DK-101567', 22000.00, 400000.00, '2025-01-06', '2025-01-07', 'weather', 'approved', 'Sofie Andersen', 'sofie.andersen@email.dk', '300488-7890', 'Havnegade 67', 'Aalborg', '9000', 'Audi', 'A4', 2021, 'Hail damage to roof and hood', FALSE, 'Storm verified by weather service data'),
    ('CLM-2025-00006', 'POL-DK-101890', 67500.00, 500000.00, '2025-01-01', '2025-01-08', 'collision', 'pending', 'Mads Christensen', 'mads.christensen@email.dk', '120795-2345', 'Strandvejen 234', 'Helsingør', '3000', 'Volkswagen', 'Passat', 2022, 'Rear-end collision at traffic light', FALSE, 'Waiting for third-party insurance response'),
    ('CLM-2025-00007', 'POL-DK-102234', 195000.00, 600000.00, '2025-01-07', '2025-01-07', 'theft', 'under_review', 'Emma Larsen', 'emma.larsen@email.dk', '250199-6789', 'Frederiksberg Allé 89', 'Frederiksberg', '1820', 'Porsche', 'Cayenne', 2023, 'Vehicle theft from shopping center', FALSE, 'Security footage requested'),
    ('CLM-2025-00008', 'POL-DK-102567', 15000.00, 350000.00, '2025-01-05', '2025-01-08', 'glass', 'approved', 'Jonas Hansen', 'jonas.hansen@email.dk', '180386-0123', 'Østerbrogade 156', 'København', '2100', 'Skoda', 'Octavia', 2019, 'Windshield crack from road debris', FALSE, 'Autoglass repair scheduled'),
    ('CLM-2025-00009', 'POL-DK-102890', 480000.00, 450000.00, '2025-01-08', '2025-01-08', 'collision', 'flagged', 'Thomas Møller', 'thomas.moller@email.dk', '050180-4567', 'Kongensgade 78', 'Esbjerg', '6700', 'Tesla', 'Model S', 2024, 'Multi-vehicle accident - total loss claimed', TRUE, 'ALERT: Claim exceeds coverage. New policy.'),
    ('CLM-2025-00010', 'POL-DK-103234', 5500.00, 200000.00, '2025-01-06', '2025-01-09', 'vandalism', 'approved', 'Laura Poulsen', 'laura.poulsen@email.dk', '280594-8901', 'Vesterbro 45', 'Randers', '8900', 'Ford', 'Focus', 2018, 'Spray paint on vehicle exterior', FALSE, 'Police report filed. Cleaning authorized.'),
    ('CLM-2025-00011', 'POL-DK-103567', 38000.00, 400000.00, '2025-01-09', '2025-01-09', 'collision', 'pending', 'Frederik Olsen', 'frederik.olsen@email.dk', '100287-2345', 'Søndergade 112', 'Kolding', '6000', 'Hyundai', 'Tucson', 2021, 'Side impact in intersection', FALSE, 'Witness statements collected'),
    ('CLM-2025-00012', 'POL-DK-103890', 92000.00, 550000.00, '2025-01-07', '2025-01-10', 'fire', 'under_review', 'Ida Rasmussen', 'ida.rasmussen@email.dk', '220691-6789', 'Langelinie 34', 'Vejle', '7100', 'Mazda', 'CX-5', 2022, 'Engine fire - cause under investigation', FALSE, 'Fire department report pending'),
    ('CLM-2025-00013', 'POL-DK-104234', 28500.00, 300000.00, '2025-01-10', '2025-01-10', 'weather', 'approved', 'Oliver Thomsen', 'oliver.thomsen@email.dk', '150398-0123', 'Stormgade 67', 'Silkeborg', '8600', 'Nissan', 'Qashqai', 2020, 'Flooding damage to interior', FALSE, 'Verified by municipal flood report'),
    ('CLM-2025-00014', 'POL-DK-104567', 175000.00, 100000.00, '2025-01-08', '2025-01-10', 'theft', 'flagged', 'Victoria Kristensen', 'victoria.kristensen@email.dk', '080585-4567', 'Parkalle 23', 'Herning', '7400', 'Land Rover', 'Range Rover', 2023, 'Reported stolen - high value vehicle', TRUE, 'ALERT: Coverage insufficient. Policy review needed.'),
    ('CLM-2025-00015', 'POL-DK-104890', 12000.00, 250000.00, '2025-01-09', '2025-01-11', 'glass', 'approved', 'Sebastian Johansen', 'sebastian.johansen@email.dk', '300492-8901', 'Industrivej 89', 'Horsens', '8700', 'Peugeot', '3008', 2021, 'Rear window shattered - unknown cause', FALSE, 'Replacement ordered'),
    ('CLM-2025-00016', 'POL-DK-105234', 55000.00, 450000.00, '2025-01-11', '2025-01-11', 'collision', 'pending', 'Mathilde Pedersen', 'mathilde.pedersen@email.dk', '200196-2345', 'Kirkegade 56', 'Roskilde', '4000', 'Kia', 'Sportage', 2022, 'Collision with cyclist - minor injuries', FALSE, 'Liability assessment in progress'),
    ('CLM-2025-00017', 'POL-DK-105567', 310000.00, 280000.00, '2025-01-10', '2025-01-11', 'fire', 'flagged', 'Christian Sørensen', 'christian.sorensen@email.dk', '110183-6789', 'Møllevej 112', 'Næstved', '4700', 'BMW', 'M4', 2024, 'Garage fire - total vehicle loss', TRUE, 'ALERT: Claim exceeds coverage. Recent policy change.'),
    ('CLM-2025-00018', 'POL-DK-105890', 7800.00, 300000.00, '2025-01-11', '2025-01-12', 'vandalism', 'approved', 'Amalie Jørgensen', 'amalie.jorgensen@email.dk', '250700-0123', 'Havnevej 34', 'Svendborg', '5700', 'Renault', 'Captur', 2020, 'Tire slashing incident', FALSE, 'Four tires replaced'),
    ('CLM-2025-00019', 'POL-DK-106234', 145000.00, 500000.00, '2025-01-12', '2025-01-12', 'collision', 'pending', 'Nikolaj Lund', 'nikolaj.lund@email.dk', '180289-4567', 'Stationsvej 78', 'Hjørring', '9800', 'Audi', 'Q7', 2023, 'Deer collision on rural road', FALSE, 'Wildlife incident documentation submitted'),
    ('CLM-2025-00020', 'POL-DK-106567', 420000.00, 400000.00, '2025-01-11', '2025-01-12', 'theft', 'flagged', 'Isabella Winther', 'isabella.winther@email.dk', '050194-8901', 'Skovvej 45', 'Viborg', '8800', 'Mercedes', 'GLE', 2024, 'Professional theft - tracking disabled', TRUE, 'ALERT: Claim exceeds coverage. Suspicious circumstances.');

-- Insert sample policy data
INSERT INTO RAW_POLICIES (
    policy_id, policy_holder_name, policy_holder_email, policy_holder_cpr,
    address, city, postal_code, policy_type, coverage_limit, premium_annual,
    policy_start_date, policy_end_date, vehicle_make, vehicle_model, vehicle_year,
    vehicle_vin, driver_age, years_licensed, previous_claims_count, risk_score
)
VALUES
    ('POL-DK-100234', 'Anders Jensen', 'anders.jensen@email.dk', '010185-1234', 'Vestergade 42', 'København', '1456', 'comprehensive', 500000.00, 8500.00, '2024-03-15', '2025-03-14', 'Volvo', 'XC60', 2022, 'YV1CZ91H5N1234567', 40, 22, 1, 'LOW'),
    ('POL-DK-100567', 'Marie Nielsen', 'marie.nielsen@email.dk', '150990-5678', 'Nørrebrogade 118', 'København', '2200', 'comprehensive', 750000.00, 12000.00, '2024-06-01', '2025-05-31', 'BMW', 'X5', 2023, 'WBAJB9C50NB123456', 35, 17, 0, 'LOW'),
    ('POL-DK-100891', 'Lars Petersen', 'lars.petersen@email.dk', '220375-9012', 'Hovedgaden 55', 'Aarhus', '8000', 'basic', 250000.00, 4500.00, '2024-01-01', '2024-12-31', 'Toyota', 'Corolla', 2020, 'JTDKN3DU5A1234567', 50, 32, 2, 'LOW'),
    ('POL-DK-101234', 'Henrik Madsen', 'henrik.madsen@email.dk', '080292-3456', 'Algade 23', 'Odense', '5000', 'basic', 300000.00, 6500.00, '2024-11-01', '2025-10-31', 'Mercedes', 'E-Class', 2024, 'WDDZF4JB5NA123456', 33, 15, 4, 'HIGH'),
    ('POL-DK-101567', 'Sofie Andersen', 'sofie.andersen@email.dk', '300488-7890', 'Havnegade 67', 'Aalborg', '9000', 'comprehensive', 400000.00, 7200.00, '2024-04-15', '2025-04-14', 'Audi', 'A4', 2021, 'WAUENAF47NN123456', 37, 19, 0, 'LOW'),
    ('POL-DK-101890', 'Mads Christensen', 'mads.christensen@email.dk', '120795-2345', 'Strandvejen 234', 'Helsingør', '3000', 'comprehensive', 500000.00, 9000.00, '2024-07-01', '2025-06-30', 'Volkswagen', 'Passat', 2022, 'WVWZZZ3CZPE123456', 30, 12, 1, 'MEDIUM'),
    ('POL-DK-102234', 'Emma Larsen', 'emma.larsen@email.dk', '250199-6789', 'Frederiksberg Allé 89', 'Frederiksberg', '1820', 'premium', 600000.00, 15000.00, '2024-09-01', '2025-08-31', 'Porsche', 'Cayenne', 2023, 'WP1AA2AY5PDA12345', 26, 8, 0, 'MEDIUM'),
    ('POL-DK-102567', 'Jonas Hansen', 'jonas.hansen@email.dk', '180386-0123', 'Østerbrogade 156', 'København', '2100', 'basic', 350000.00, 5500.00, '2024-02-15', '2025-02-14', 'Skoda', 'Octavia', 2019, 'TMBJG7NE5K0123456', 39, 21, 1, 'LOW'),
    ('POL-DK-102890', 'Thomas Møller', 'thomas.moller@email.dk', '050180-4567', 'Kongensgade 78', 'Esbjerg', '6700', 'comprehensive', 450000.00, 18500.00, '2025-01-01', '2025-12-31', 'Tesla', 'Model S', 2024, '5YJSA1E45NF123456', 45, 27, 0, 'HIGH'),
    ('POL-DK-103234', 'Laura Poulsen', 'laura.poulsen@email.dk', '280594-8901', 'Vesterbro 45', 'Randers', '8900', 'basic', 200000.00, 3800.00, '2024-05-01', '2025-04-30', 'Ford', 'Focus', 2018, '1FADP3F27JL123456', 31, 13, 0, 'LOW'),
    ('POL-DK-103567', 'Frederik Olsen', 'frederik.olsen@email.dk', '100287-2345', 'Søndergade 112', 'Kolding', '6000', 'comprehensive', 400000.00, 7800.00, '2024-08-15', '2025-08-14', 'Hyundai', 'Tucson', 2021, 'KM8J3CA46MU123456', 38, 20, 1, 'LOW'),
    ('POL-DK-103890', 'Ida Rasmussen', 'ida.rasmussen@email.dk', '220691-6789', 'Langelinie 34', 'Vejle', '7100', 'comprehensive', 550000.00, 10500.00, '2024-03-01', '2025-02-28', 'Mazda', 'CX-5', 2022, 'JM3KFBDM5N0123456', 34, 16, 0, 'LOW'),
    ('POL-DK-104234', 'Oliver Thomsen', 'oliver.thomsen@email.dk', '150398-0123', 'Stormgade 67', 'Silkeborg', '8600', 'basic', 300000.00, 5200.00, '2024-06-15', '2025-06-14', 'Nissan', 'Qashqai', 2020, 'JN1TBNT32Z0123456', 27, 9, 0, 'LOW'),
    ('POL-DK-104567', 'Victoria Kristensen', 'victoria.kristensen@email.dk', '080585-4567', 'Parkalle 23', 'Herning', '7400', 'basic', 100000.00, 8500.00, '2024-12-01', '2025-11-30', 'Land Rover', 'Range Rover', 2023, 'SALGS2SE5NA123456', 40, 22, 2, 'HIGH'),
    ('POL-DK-104890', 'Sebastian Johansen', 'sebastian.johansen@email.dk', '300492-8901', 'Industrivej 89', 'Horsens', '8700', 'comprehensive', 250000.00, 4800.00, '2024-04-01', '2025-03-31', 'Peugeot', '3008', 2021, 'VF3MCYHZRML123456', 33, 15, 0, 'LOW'),
    ('POL-DK-105234', 'Mathilde Pedersen', 'mathilde.pedersen@email.dk', '200196-2345', 'Kirkegade 56', 'Roskilde', '4000', 'comprehensive', 450000.00, 8200.00, '2024-07-15', '2025-07-14', 'Kia', 'Sportage', 2022, 'KNDPM3AC5N7123456', 29, 11, 0, 'LOW'),
    ('POL-DK-105567', 'Christian Sørensen', 'christian.sorensen@email.dk', '110183-6789', 'Møllevej 112', 'Næstved', '4700', 'basic', 280000.00, 14000.00, '2024-12-15', '2025-12-14', 'BMW', 'M4', 2024, 'WBS33AZ09NCK12345', 42, 24, 3, 'HIGH'),
    ('POL-DK-105890', 'Amalie Jørgensen', 'amalie.jorgensen@email.dk', '250700-0123', 'Havnevej 34', 'Svendborg', '5700', 'basic', 300000.00, 4200.00, '2024-01-15', '2025-01-14', 'Renault', 'Captur', 2020, 'VF17RHN0A63123456', 25, 7, 0, 'LOW'),
    ('POL-DK-106234', 'Nikolaj Lund', 'nikolaj.lund@email.dk', '180289-4567', 'Stationsvej 78', 'Hjørring', '9800', 'comprehensive', 500000.00, 11000.00, '2024-09-15', '2025-09-14', 'Audi', 'Q7', 2023, 'WAUZZZ4M6ND123456', 36, 18, 1, 'LOW'),
    ('POL-DK-106567', 'Isabella Winther', 'isabella.winther@email.dk', '050194-8901', 'Skovvej 45', 'Viborg', '8800', 'comprehensive', 400000.00, 13500.00, '2024-10-01', '2025-09-30', 'Mercedes', 'GLE', 2024, '4JGFB4JB5NA123456', 31, 13, 2, 'HIGH');

-- ============================================================================
-- SECTION 6: VERIFICATION
-- ============================================================================

-- Verify data load
SELECT 'RAW_CLAIMS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_CLAIMS
UNION ALL
SELECT 'RAW_POLICIES' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_POLICIES;

-- Preview raw claims
SELECT * FROM RAW_CLAIMS LIMIT 5;

-- Preview raw policies
SELECT * FROM RAW_POLICIES LIMIT 5;

-- Show tables created
SHOW TABLES IN SCHEMA INSURANCECO.RAW;

SELECT 'Raw data load complete!' AS STATUS,
       (SELECT COUNT(*) FROM RAW_CLAIMS) AS CLAIMS_LOADED,
       (SELECT COUNT(*) FROM RAW_POLICIES) AS POLICIES_LOADED;
