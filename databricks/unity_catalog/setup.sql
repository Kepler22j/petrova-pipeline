-- ============================================================
-- PETROVA – Unity Catalog Setup (Databricks DE Associate Exam)
-- Topics: Catalog, Schema, Managed/External tables, Grants, Lineage
-- ============================================================

-- 1. Three-level namespace: catalog.schema.table (exam topic)
CREATE CATALOG IF NOT EXISTS petrova_prod;
USE CATALOG petrova_prod;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw/immutable data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and validated';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business-ready aggregates';

-- 2. Managed table (Unity Catalog manages storage)
CREATE TABLE IF NOT EXISTS petrova_prod.bronze.sensor_readings (
    sensor_id       STRING      NOT NULL,
    sensor_name     STRING,
    reading_value   DOUBLE,
    reading_timestamp TIMESTAMP NOT NULL,
    status          STRING,
    process_area    STRING
)
USING DELTA
COMMENT 'Managed Delta table – UC controls storage location';

-- 3. External table (data stored at external location)
CREATE TABLE IF NOT EXISTS petrova_prod.bronze.sap_orders_external (
    order_number    STRING,
    order_date      DATE,
    net_value       DOUBLE
)
USING DELTA
LOCATION 'abfss://petrova@datalake.dfs.core.windows.net/bronze/sap_orders/'
COMMENT 'External Delta table – data in ADLS Gen2';

-- 4. External Location + Storage Credential (exam topic)
-- CREATE STORAGE CREDENTIAL petrova_azure_cred
--     WITH (AZURE_MANAGED_IDENTITY = '/subscriptions/.../resourceGroups/.../providers/.../userAssignedIdentities/petrova-identity');
--
-- CREATE EXTERNAL LOCATION petrova_landing
--     URL 'abfss://landing@petrovadatalake.dfs.core.windows.net/'
--     WITH (STORAGE CREDENTIAL petrova_azure_cred);

-- 5. Grants (exam topic: GRANT/REVOKE on catalog objects)
-- Catalog-level
GRANT USE CATALOG ON CATALOG petrova_prod TO `data-analysts`;
GRANT USE SCHEMA ON SCHEMA petrova_prod.gold TO `data-analysts`;
GRANT SELECT ON SCHEMA petrova_prod.gold TO `data-analysts`;

-- Table-level
GRANT SELECT ON TABLE petrova_prod.gold.fct_daily_sensor_kpi TO `data-analysts`;

-- Schema-level (all current + future tables)
GRANT ALL PRIVILEGES ON SCHEMA petrova_prod.bronze TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA petrova_prod.silver TO `data-engineers`;

-- 6. Data Lineage (exam topic: automatic in Unity Catalog)
-- Unity Catalog automatically tracks lineage across:
--   - Table-to-table dependencies
--   - Column-level lineage
--   - Notebook/job provenance
-- Viewable in Catalog Explorer UI → Lineage tab

-- 7. Delta table properties (exam topic)
ALTER TABLE petrova_prod.silver.sensor_readings_cleaned SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',           -- CDF for downstream CDC
    'delta.autoOptimize.optimizeWrite' = 'true',      -- Auto compaction on write
    'delta.autoOptimize.autoCompact' = 'true',        -- Auto small file compaction
    'delta.logRetentionDuration' = 'interval 30 days', -- Transaction log retention
    'delta.deletedFileRetentionDuration' = 'interval 7 days'  -- VACUUM threshold
);

-- 8. OPTIMIZE and VACUUM (exam topic)
OPTIMIZE petrova_prod.silver.sensor_readings_cleaned ZORDER BY (sensor_id, reading_timestamp);
VACUUM petrova_prod.silver.sensor_readings_cleaned RETAIN 168 HOURS;  -- 7 days

-- 9. DESCRIBE and history (exam topic)
DESCRIBE DETAIL petrova_prod.gold.fct_daily_sensor_kpi;
DESCRIBE HISTORY petrova_prod.gold.fct_daily_sensor_kpi;

-- 10. Delta Time Travel on Databricks
SELECT * FROM petrova_prod.gold.fct_daily_sensor_kpi VERSION AS OF 5;
SELECT * FROM petrova_prod.gold.fct_daily_sensor_kpi TIMESTAMP AS OF '2025-06-01';
RESTORE TABLE petrova_prod.gold.fct_daily_sensor_kpi TO VERSION AS OF 5;
