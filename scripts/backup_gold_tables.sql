-- ============================================================
-- PETROVA – Gold Layer Backup Script (Zero-Copy Clone)
-- Run before major deployments or schema changes
-- ============================================================

SET backup_suffix = TO_CHAR(CURRENT_DATE(), 'YYYYMMDD');

-- Backup all Gold tables via Zero-Copy Clone
CREATE TABLE IF NOT EXISTS PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI_BACKUP_$backup_suffix
    CLONE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI;

CREATE TABLE IF NOT EXISTS PETROVA_PROD.GOLD.FCT_DAILY_REVENUE_BACKUP_$backup_suffix
    CLONE PETROVA_PROD.GOLD.FCT_DAILY_REVENUE;

CREATE TABLE IF NOT EXISTS PETROVA_PROD.GOLD.DIM_EQUIPMENT_BACKUP_$backup_suffix
    CLONE PETROVA_PROD.GOLD.DIM_EQUIPMENT;

CREATE TABLE IF NOT EXISTS PETROVA_PROD.GOLD.DIM_VENDOR_BACKUP_$backup_suffix
    CLONE PETROVA_PROD.GOLD.DIM_VENDOR;

-- Cleanup: drop backups older than 30 days
-- (Manual step – list and review before dropping)
SHOW TABLES LIKE '%_BACKUP_%' IN SCHEMA PETROVA_PROD.GOLD;
