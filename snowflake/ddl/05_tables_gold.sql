-- ============================================================
-- PETROVA – Gold Layer Tables (Business-Ready / Immutable)
-- Gold Immutability: 7 Commandments (Agent.MD Section 6)
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA GOLD;

CREATE TABLE IF NOT EXISTS FCT_DAILY_SENSOR_KPI (
    kpi_sk              VARCHAR(64)     NOT NULL,
    sensor_id           VARCHAR(50)     NOT NULL,
    sensor_name         VARCHAR(200),
    equipment_name      VARCHAR(200),
    process_area        VARCHAR(100),
    kpi_date            DATE            NOT NULL,
    total_readings      INTEGER,
    avg_reading         FLOAT,
    min_reading         FLOAT,
    max_reading         FLOAT,
    stddev_reading      FLOAT,
    median_reading      FLOAT,
    p95_reading         FLOAT,
    warn_count          INTEGER,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_sensor_kpi PRIMARY KEY (kpi_sk)
)
CLUSTER BY (kpi_date)
COMMENT = 'Gold – daily sensor KPI aggregations. RBAC-protected.';

CREATE TABLE IF NOT EXISTS FCT_DAILY_REVENUE (
    revenue_sk          VARCHAR(64)     NOT NULL,
    revenue_date        DATE            NOT NULL,
    material_group      VARCHAR(20),
    order_count         INTEGER,
    total_quantity      NUMBER(18,4),
    total_revenue       NUMBER(18,2),
    avg_order_value     NUMBER(18,2),
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_daily_rev PRIMARY KEY (revenue_sk)
)
CLUSTER BY (revenue_date)
COMMENT = 'Gold – daily revenue by material group.';

CREATE TABLE IF NOT EXISTS DIM_EQUIPMENT (
    equipment_sk        VARCHAR(64)     NOT NULL,
    equipment_id        VARCHAR(20),
    equipment_name      VARCHAR(200),
    equipment_type      VARCHAR(50),
    location            VARCHAR(200),
    install_date        DATE,
    status              VARCHAR(20),
    days_since_install  INTEGER,
    lifecycle_stage     VARCHAR(20),
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_equip PRIMARY KEY (equipment_sk)
)
COMMENT = 'Gold – equipment dimension with lifecycle.';

CREATE TABLE IF NOT EXISTS DIM_VENDOR (
    vendor_sk           VARCHAR(64)     NOT NULL,
    vendor_number       VARCHAR(20),
    vendor_name         VARCHAR(200),
    country             VARCHAR(5),
    payment_terms       VARCHAR(20),
    is_current          BOOLEAN,
    valid_from          TIMESTAMP_NTZ,
    valid_to            TIMESTAMP_NTZ,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_vendor PRIMARY KEY (vendor_sk)
)
COMMENT = 'Gold – vendor dimension (SCD2 current + history).';
