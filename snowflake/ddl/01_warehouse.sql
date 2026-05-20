-- ============================================================
-- PETROVA – Snowflake Warehouse Setup
-- ============================================================

-- Development warehouse (X-Small for cost control)
CREATE WAREHOUSE IF NOT EXISTS PETROVA_DEV_WH
    WAREHOUSE_SIZE  = 'X-SMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    COMMENT         = 'PETROVA dev/test workloads';

-- Production warehouse (Small, multi-cluster for concurrency)
CREATE WAREHOUSE IF NOT EXISTS PETROVA_PROD_WH
    WAREHOUSE_SIZE  = 'SMALL'
    AUTO_SUSPEND    = 120
    AUTO_RESUME     = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY  = 'STANDARD'
    COMMENT         = 'PETROVA production pipeline + BI queries';

-- ETL warehouse (Medium, dedicated for heavy transforms)
CREATE WAREHOUSE IF NOT EXISTS PETROVA_ETL_WH
    WAREHOUSE_SIZE  = 'MEDIUM'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    COMMENT         = 'PETROVA ETL / dbt transformations';
