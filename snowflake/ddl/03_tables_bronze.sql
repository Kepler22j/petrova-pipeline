-- ============================================================
-- PETROVA – Bronze Layer Tables (Raw / Immutable)
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS SENSOR_READINGS_PARQUET (
    sensor_id           VARCHAR(50)     NOT NULL,
    sensor_name         VARCHAR(200),
    equipment_name      VARCHAR(200),
    reading_value       FLOAT,
    reading_timestamp   TIMESTAMP_NTZ   NOT NULL,
    status              VARCHAR(20),
    process_area        VARCHAR(100),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50)
)
CLUSTER BY (reading_timestamp::DATE)
COMMENT = 'Raw sensor readings ingested from Parquet via ADF/Snowpipe';

CREATE TABLE IF NOT EXISTS SAP_ORDERS_RAW (
    order_number        VARCHAR(20)     NOT NULL,
    order_date          DATE,
    customer_number     VARCHAR(20),
    material_number     VARCHAR(20),
    order_quantity      NUMBER(18,4),
    net_value           NUMBER(18,2),
    currency            VARCHAR(5),
    plant               VARCHAR(10),
    sales_org           VARCHAR(10),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_system      VARCHAR(50)     DEFAULT 'SAP_BODS'
)
COMMENT = 'Raw SAP sales orders via SAP BODS extraction';

CREATE TABLE IF NOT EXISTS SAP_MATERIALS_RAW (
    material_number     VARCHAR(20)     NOT NULL,
    material_description VARCHAR(500),
    material_group      VARCHAR(20),
    unit_of_measure     VARCHAR(10),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw SAP material master';

CREATE TABLE IF NOT EXISTS SAP_VENDORS_RAW (
    vendor_number       VARCHAR(20)     NOT NULL,
    vendor_name         VARCHAR(200),
    country             VARCHAR(5),
    payment_terms       VARCHAR(20),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw SAP vendor master';

CREATE TABLE IF NOT EXISTS EQUIPMENT_MASTER_RAW (
    equipment_id        VARCHAR(20)     NOT NULL,
    equipment_name      VARCHAR(200),
    equipment_type      VARCHAR(50),
    location            VARCHAR(200),
    install_date        DATE,
    status              VARCHAR(20),
    _loaded_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw equipment master data';
