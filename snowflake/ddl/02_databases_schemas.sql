-- ============================================================
-- PETROVA 300K – Database & Schema Setup (Medallion Architecture)
-- ============================================================

-- Production database
CREATE DATABASE IF NOT EXISTS PETROVA_PROD
    COMMENT = 'PETROVA 300K production data platform';

USE DATABASE PETROVA_PROD;

-- Medallion schemas
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw/immutable ingestion layer';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Cleaned, validated, SCD2 layer';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Business-ready, RBAC-protected layer';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Transient staging area for ELT loads';

CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'Pipeline audit logs and lineage tracking';

-- Development database
CREATE DATABASE IF NOT EXISTS PETROVA_DEV
    COMMENT = 'PETROVA development / sandbox';

USE DATABASE PETROVA_DEV;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
