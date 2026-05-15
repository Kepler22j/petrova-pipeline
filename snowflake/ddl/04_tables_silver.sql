-- ============================================================
-- PETROVA – Silver Layer Tables (Cleaned / SCD2)
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS SENSOR_READINGS_CLEANED (
    sensor_reading_sk   VARCHAR(64)     NOT NULL,
    sensor_id           VARCHAR(50)     NOT NULL,
    sensor_name         VARCHAR(200),
    equipment_name      VARCHAR(200),
    reading_value       FLOAT,
    reading_timestamp   TIMESTAMP_NTZ   NOT NULL,
    status              VARCHAR(20),
    process_area        VARCHAR(100),
    quality_flag        VARCHAR(10),
    is_valid            BOOLEAN,
    loaded_at           TIMESTAMP_NTZ,
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_sensor_clean PRIMARY KEY (sensor_reading_sk)
)
CLUSTER BY (reading_timestamp::DATE)
COMMENT = 'Cleaned sensor readings with quality flags (Silver Gate passed)';

CREATE TABLE IF NOT EXISTS ORDERS_VALIDATED (
    order_sk            VARCHAR(64)     NOT NULL,
    order_number        VARCHAR(20)     NOT NULL,
    order_date          DATE,
    customer_number     VARCHAR(20),
    material_number     VARCHAR(20),
    material_description VARCHAR(500),
    material_group      VARCHAR(20),
    order_quantity      NUMBER(18,4),
    net_value           NUMBER(18,2),
    quality_flag        VARCHAR(10),
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_order_val PRIMARY KEY (order_sk)
)
COMMENT = 'Validated orders enriched with material data';

CREATE TABLE IF NOT EXISTS VENDORS_SCD2 (
    vendor_sk           VARCHAR(64)     NOT NULL,
    vendor_number       VARCHAR(20)     NOT NULL,
    vendor_name         VARCHAR(200),
    country             VARCHAR(5),
    payment_terms       VARCHAR(20),
    row_hash            VARCHAR(64),
    is_current          BOOLEAN         DEFAULT TRUE,
    valid_from          TIMESTAMP_NTZ,
    valid_to            TIMESTAMP_NTZ   DEFAULT '9999-12-31',
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_vendor_scd2 PRIMARY KEY (vendor_sk)
)
COMMENT = 'SCD Type 2 vendor dimension – full history tracking';
