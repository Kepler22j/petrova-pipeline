-- ============================================================
-- PETROVA 300K – Data Sharing & Security (SnowPro Core Exam)
-- Topics: Secure Shares, Reader Accounts, Network Policies,
--         Row Access Policies, Masking Policies, Encryption
-- ============================================================
USE DATABASE PETROVA_PROD;

-- ═══ DATA SHARING ═══

-- 1. Create a Share (provider side)
CREATE SHARE IF NOT EXISTS PETROVA_GOLD_SHARE
    COMMENT = 'Shared Gold layer KPIs for partner access';

GRANT USAGE ON DATABASE PETROVA_PROD TO SHARE PETROVA_GOLD_SHARE;
GRANT USAGE ON SCHEMA PETROVA_PROD.GOLD TO SHARE PETROVA_GOLD_SHARE;
GRANT SELECT ON TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI TO SHARE PETROVA_GOLD_SHARE;
GRANT SELECT ON TABLE PETROVA_PROD.GOLD.FCT_DAILY_REVENUE TO SHARE PETROVA_GOLD_SHARE;

-- 2. Create Secure View for sharing (hides underlying query logic)
CREATE SECURE VIEW GOLD.SV_PARTNER_KPI AS
    SELECT
        kpi_date,
        sensor_name,
        avg_reading,
        p95_reading,
        total_readings
    FROM GOLD.FCT_DAILY_SENSOR_KPI
    WHERE kpi_date >= DATEADD('day', -90, CURRENT_DATE());

GRANT SELECT ON VIEW GOLD.SV_PARTNER_KPI TO SHARE PETROVA_GOLD_SHARE;

-- 3. Add consumer account to share
ALTER SHARE PETROVA_GOLD_SHARE ADD ACCOUNTS = PARTNER_ACCOUNT;

-- 4. Reader Account (for consumers without Snowflake – exam topic)
CREATE MANAGED ACCOUNT PETROVA_READER_ACCT
    ADMIN_NAME = 'reader_admin',
    ADMIN_PASSWORD = 'ComplexP@ss123!',
    TYPE = READER
    COMMENT = 'Reader account for non-Snowflake partners';

-- 5. Consumer side: create database from share
-- CREATE DATABASE PARTNER_DATA FROM SHARE PROVIDER_ACCOUNT.PETROVA_GOLD_SHARE;

-- ═══ NETWORK SECURITY ═══

-- 6. Network Policy (IP allowlist/blocklist – exam topic)
CREATE NETWORK POLICY IF NOT EXISTS PETROVA_NETWORK_POLICY
    ALLOWED_IP_LIST = ('203.0.113.0/24', '198.51.100.0/24')  -- Office IPs
    BLOCKED_IP_LIST = ('192.0.2.100')                          -- Known bad IP
    COMMENT = 'Restrict access to corporate network';

-- Apply to account
ALTER ACCOUNT SET NETWORK_POLICY = PETROVA_NETWORK_POLICY;

-- Apply to specific user
ALTER USER DATA_ANALYST SET NETWORK_POLICY = PETROVA_NETWORK_POLICY;

-- ═══ COLUMN-LEVEL SECURITY ═══

-- 7. Dynamic Data Masking Policy (exam topic)
CREATE MASKING POLICY IF NOT EXISTS GOLD.MASK_PII_EMAIL AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('PETROVA_ADMIN', 'PETROVA_ENGINEER')
            THEN val                                    -- Full access
        WHEN CURRENT_ROLE() = 'PETROVA_ANALYST'
            THEN REGEXP_REPLACE(val, '.+@', '****@')    -- Masked
        ELSE '**REDACTED**'                              -- Hidden
    END
    COMMENT = 'PII masking: full for admin/engineer, partial for analyst';

-- Apply masking policy to column
-- ALTER TABLE GOLD.DIM_CUSTOMER MODIFY COLUMN email
--     SET MASKING POLICY GOLD.MASK_PII_EMAIL;

-- 8. Row Access Policy (exam topic)
CREATE ROW ACCESS POLICY IF NOT EXISTS GOLD.RAP_REGION_FILTER AS
    (region_col VARCHAR) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('PETROVA_ADMIN') THEN TRUE     -- See all regions
        WHEN CURRENT_ROLE() = 'PETROVA_ANALYST'
            AND region_col = 'APAC' THEN TRUE                   -- APAC only
        ELSE FALSE
    END
    COMMENT = 'Region-based row filtering for analysts';

-- 9. Encryption (exam topic: always-on, end-to-end)
-- Snowflake encryption hierarchy:
--   Root Key (HSM) → Account Master Key → Table Master Key → File Key
-- Tri-Secret Secure: customer-managed key + Snowflake key = composite master key
-- All data encrypted at rest (AES-256) and in transit (TLS 1.2)

-- ═══ RESOURCE MONITORS ═══

-- 10. Resource Monitor (exam topic: cost control)
CREATE RESOURCE MONITOR IF NOT EXISTS PETROVA_MONTHLY_MONITOR
    WITH CREDIT_QUOTA = 500
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 75 PERCENT DO NOTIFY                     -- Alert at 75%
        ON 90 PERCENT DO NOTIFY                     -- Alert at 90%
        ON 100 PERCENT DO SUSPEND                   -- Suspend queries at 100%
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;        -- Kill running queries at 110%

ALTER WAREHOUSE PETROVA_PROD_WH
    SET RESOURCE_MONITOR = PETROVA_MONTHLY_MONITOR;
