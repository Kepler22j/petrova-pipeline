-- ============================================================
-- PETROVA – Pipeline Notification Procedure
-- Sends email alerts on pipeline failure via Snowflake + SMTP
-- ============================================================

CREATE OR REPLACE PROCEDURE PETROVA_PROD.AUDIT.SP_PIPELINE_NOTIFICATION(
    PIPELINE_NAME VARCHAR,
    STATUS VARCHAR,
    ERROR_MSG VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Log to audit table
    snowflake.execute({
        sqlText: `INSERT INTO PETROVA_PROD.AUDIT.PIPELINE_LOG
                  (procedure_name, status, error_message)
                  VALUES (?, ?, ?)`,
        binds: [PIPELINE_NAME, STATUS, ERROR_MSG]
    });

    // If failure, trigger email notification
    if (STATUS === 'FAILURE') {
        snowflake.execute({
            sqlText: `CALL SYSTEM$SEND_EMAIL(
                'petrova_email_integration',
                '${process.env.ALERT_EMAIL || "alerts@petrova.local"}',
                'PETROVA ALERT: ${PIPELINE_NAME} FAILED',
                'Pipeline: ${PIPELINE_NAME}\\nStatus: FAILURE\\nError: ${ERROR_MSG || "Unknown"}\\nTime: ' || CURRENT_TIMESTAMP()::VARCHAR
            )`
        });
    }

    return STATUS + ': ' + PIPELINE_NAME;
$$;

-- ============================================================
-- Alert threshold monitoring view
-- ============================================================
CREATE OR REPLACE VIEW PETROVA_PROD.AUDIT.V_PIPELINE_ALERTS AS
SELECT
    procedure_name,
    status,
    error_message,
    executed_at,
    DATEDIFF('minute', LAG(executed_at) OVER (
        PARTITION BY procedure_name ORDER BY executed_at
    ), executed_at) AS minutes_since_last_run,
    CASE
        WHEN status = 'FAILURE' THEN 'CRITICAL'
        WHEN DATEDIFF('hour', executed_at, CURRENT_TIMESTAMP()) > 24 THEN 'WARNING'
        ELSE 'OK'
    END AS alert_level
FROM PETROVA_PROD.AUDIT.PIPELINE_LOG
QUALIFY ROW_NUMBER() OVER (PARTITION BY procedure_name ORDER BY executed_at DESC) = 1;
