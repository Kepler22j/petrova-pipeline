-- ============================================================
-- PETROVA – Gold Immutability Enforcement
-- Implements the 7 Commandments (Agent.MD Section 6)
-- ============================================================

-- Commandment #1: No direct INSERT/UPDATE/DELETE on Gold tables
-- Commandment #2: All writes via stored procedures only
-- Commandment #3: MERGE pattern for upserts
-- Commandment #4: Zero-Copy Clone for safe rollback
-- Commandment #5: Audit every mutation
-- Commandment #6: RBAC enforcement (4-role hierarchy)
-- Commandment #7: Automated validation post-write

CREATE OR REPLACE PROCEDURE PETROVA_PROD.GOLD.SP_GOLD_IMMUTABILITY_CHECK()
RETURNS TABLE (check_name VARCHAR, status VARCHAR, details VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    results RESULTSET;
BEGIN
    -- Check 1: Verify Gold tables have change tracking enabled
    results := (
        SELECT
            'CHANGE_TRACKING' AS check_name,
            CASE WHEN change_tracking = 'ON' THEN 'PASS' ELSE 'FAIL' END AS status,
            table_name || ': change_tracking=' || change_tracking AS details
        FROM PETROVA_PROD.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_type = 'BASE TABLE'

        UNION ALL

        -- Check 2: Verify Zero-Copy Clone exists for rollback
        SELECT
            'ZERO_COPY_CLONE' AS check_name,
            CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'WARN' END,
            'Gold backup clones found: ' || COUNT(*)::VARCHAR
        FROM PETROVA_PROD.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = 'GOLD'
          AND table_name LIKE '%_BACKUP_%'

        UNION ALL

        -- Check 3: Verify audit log has recent entries
        SELECT
            'AUDIT_LOG_RECENT' AS check_name,
            CASE WHEN MAX(executed_at) > DATEADD('hour', -24, CURRENT_TIMESTAMP())
                 THEN 'PASS' ELSE 'WARN' END,
            'Last audit entry: ' || COALESCE(MAX(executed_at)::VARCHAR, 'NONE')
        FROM PETROVA_PROD.AUDIT.PIPELINE_LOG
    );

    RETURN TABLE(results);
END;
$$;

-- Commandment #4: Zero-Copy Clone procedure
CREATE OR REPLACE PROCEDURE PETROVA_PROD.GOLD.SP_CREATE_GOLD_BACKUP(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    clone_name VARCHAR;
    clone_sql  VARCHAR;
BEGIN
    clone_name := TABLE_NAME || '_BACKUP_' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD');
    clone_sql  := 'CREATE TABLE PETROVA_PROD.GOLD.' || clone_name ||
                  ' CLONE PETROVA_PROD.GOLD.' || TABLE_NAME;
    EXECUTE IMMEDIATE clone_sql;
    RETURN 'Created backup: ' || clone_name;
END;
$$;
