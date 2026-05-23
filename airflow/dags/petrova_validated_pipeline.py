"""
PETROVA — Validated Data Pipeline DAG
══════════════════════════════════════
Triple-Orchestration: Airflow + ADF + dbt
3-Gate Validation: Bronze Gate → Silver Gate → Gold Gate (SPC)

Failover Strategy:
  Layer A — Partial Async:  trigger_rule='all_done' + quarantine routing
  Layer B — Cached Gold:    Zero-Copy Clone snapshot before each rebuild
  Layer C — Dual-Platform:  Databricks + Snowflake cross-failover

Notification Chain:
  CRITICAL → PagerDuty (30-min escalation) + Email + Slack
  WARNING  → Email + Slack
  OK       → Streamlit dashboard refresh only
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Notification callbacks ──
def alert_on_failure(context):
    """Airflow on_failure_callback — sends PagerDuty + email on any task failure."""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    log_url = context['task_instance'].log_url
    print(f"ALERT: Task {task_id} in {dag_id} failed at {exec_date}")
    print(f"Log URL: {log_url}")
    # In production:
    # from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyHook
    # hook = PagerdutyHook(pagerduty_conn_id='pagerduty_default')
    # hook.create_event(
    #     summary=f"PETROVA Pipeline CRITICAL: {task_id} failed",
    #     severity='critical',
    #     source='airflow-petrova',
    #     routing_key=Variable.get('PAGERDUTY_ROUTING_KEY'),
    # )


def alert_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA miss callback — triggers when pipeline exceeds max execution time."""
    print(f"SLA MISS: PETROVA pipeline exceeded execution time limit")
    # In production: PagerDuty critical alert


# ── Default args with failover config ──
default_args = {
    'owner': 'jay-pechnarai',
    'depends_on_past': False,
    'email': ['pechnarai.jakapong@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,                              # Layer A: 3 retries before giving up
    'retry_delay': timedelta(minutes=5),       # 5-min exponential backoff
    'retry_exponential_backoff': True,         # 5 → 10 → 20 min
    'max_retry_delay': timedelta(minutes=30),  # Cap at 30 min
    'execution_timeout': timedelta(hours=2),   # Kill task after 2 hours
    'on_failure_callback': alert_on_failure,   # Layer C: PagerDuty on failure
    'start_date': datetime(2026, 5, 16),
}


# ── Gate validation functions ──
def bronze_gate_check(**context):
    """
    Bronze Gate: Schema validation + null checks on raw data.
    Uses: pyspark — col().isNull(), cast(), between()
    Fail: Quarantine bad records, continue processing clean records.
    """
    print("Running Bronze Gate validation...")
    print("  #1 Null Detection:   pyspark col().isNull() → flag FAIL")
    print("  #2 Type Casting:     pyspark col().cast(DoubleType()) → flag FAIL")
    print("  #3 Range Validation: pyspark col().between(-999, 9999) → flag WARN")
    # In production:
    # from great_expectations.checkpoint import Checkpoint
    # result = Checkpoint(name='bronze_gate').run()
    # fail_count = result.statistics['unsuccessful_validations']
    # context['ti'].xcom_push(key='bronze_fail_count', value=fail_count)
    validation_passed = True  # Simulated
    if validation_passed:
        return 'bronze_gate_passed'
    else:
        return 'bronze_gate_failed'


def silver_gate_check(**context):
    """
    Silver Gate: Data quality + business rules + referential integrity.
    Uses: pyspark — Window, ROW_NUMBER | dbt — relationships test, MERGE
    Fail: Quarantine bad records, promote PASS/WARN only.
    """
    print("Running Silver Gate validation...")
    print("  #4 Status Filter:   pyspark filter(col('status') != 'CANCELLED')")
    print("  #5 Dedup:           pyspark Window.partitionBy() + ROW_NUMBER()")
    print("  #6 Derived Cols:    pyspark withColumn() + DATEDIFF()")
    print("  #7 SCD2 Merge:      dbt generate_surrogate_key() + MERGE INTO")
    print("  #8 Ref Integrity:   dbt relationships test (LEFT JOIN + IS NULL)")
    print("  #9 Agg Guards:      dbt HAVING COUNT(*) > 0")
    print("  #10 Late Data:      spark withWatermark() + trigger(once=True)")
    validation_passed = True  # Simulated
    if validation_passed:
        return 'silver_gate_passed'
    else:
        return 'silver_gate_failed'


def gold_gate_spc_check(**context):
    """
    Gold Gate (SPC): Statistical Process Control — 6 alerts from 3 primitives.
    Uses: dbt — LAG(), STDDEV(), CASE WHEN composite severity
    Fail: CRITICAL → PagerDuty | WARNING → Email | OK → Dashboard only
    """
    print("Running Gold Gate SPC validation...")
    print("  Alert 1: STABILITY   — STDDEV thresholds (STABLE/NORMAL/UNSTABLE)")
    print("  Alert 2: SIGNAL_NOISE — CV = stddev/avg (CLEAN/NOISY/VERY_NOISY)")
    print("  Alert 3: OUTLIER     — readings > 2x stddev from mean")
    print("  Alert 4: RANGE       — outside avg ± 1 stddev band")
    print("  Alert 5: VOLATILITY  — stddev trending up vs prev day (LAG)")
    print("  Alert 6: SPIKE       — max_reading delta > 2x stddev (LAG)")
    print("  Composite: UNSTABLE + NOISY = CRITICAL | single trigger = WARNING")
    # In production:
    # spc_result = context['ti'].xcom_pull(task_ids='dbt_run_marts')
    # critical_count = query("SELECT COUNT(*) FROM gold.fct_sensor_alerts WHERE alert_severity='CRITICAL'")
    # context['ti'].xcom_push(key='critical_alerts', value=critical_count)
    validation_passed = True  # Simulated
    if validation_passed:
        return 'gold_gate_passed'
    else:
        return 'gold_gate_failed'


def cache_gold_snapshot(**context):
    """
    Layer B: Create Zero-Copy Clone of Gold tables before rebuild.
    If Gold build fails, BI tools fall back to this cached snapshot.
    """
    print("Creating Snowflake Zero-Copy Clone of Gold tables...")
    print("  CREATE TABLE gold.fct_daily_sensor_kpi_cache CLONE gold.fct_daily_sensor_kpi;")
    print("  CREATE TABLE gold.fct_sensor_alerts_cache CLONE gold.fct_sensor_alerts;")
    print("  CREATE TABLE gold.fct_daily_revenue_cache CLONE gold.fct_daily_revenue;")
    print("  Metadata: _is_cached=TRUE, _cache_timestamp=CURRENT_TIMESTAMP()")
    # In production:
    # from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
    # SnowflakeOperator(sql="CREATE OR REPLACE TABLE ... CLONE ...", ...)


def quarantine_failed_records(layer, **context):
    """Route failed records to quarantine Delta table for review."""
    print(f"Routing {layer} failed records to quarantine table...")
    print(f"  INSERT INTO audit.quarantine_{layer} SELECT * WHERE quality_flag='FAIL'")
    print(f"  Quarantine row count pushed to XCom for monitoring")
    # In production:
    # quarantine_count = spark.sql(f"SELECT COUNT(*) FROM audit.quarantine_{layer}").first()[0]
    # if quarantine_count > 1000:  # threshold for alerting
    #     raise AirflowException(f"Quarantine spike: {quarantine_count} records")


# ── DAG Definition ──
with DAG(
    dag_id='petrova_validated_pipeline',
    default_args=default_args,
    description='PETROVA end-to-end pipeline: 3-Gate Validation + Failover + SPC Alerts',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=alert_on_sla_miss,
    tags=['petrova', 'production', 'medallion', 'validated', 'spc'],
) as dag:

    # ═══════════════════════════════════════════
    # STAGE 1: BRONZE (Raw Ingestion)
    # ═══════════════════════════════════════════
    start = EmptyOperator(task_id='start')

    # Parallel ingestion — all sources run simultaneously
    ingest_sensor_data = BashOperator(
        task_id='ingest_sensor_data',
        bash_command='echo "Auto Loader: IoT sensor Parquet → Bronze Delta table (400K+ records)"',
        # In production: DatabricksRunNowOperator(job_id=SENSOR_INGEST_JOB)
    )

    ingest_sap_orders = BashOperator(
        task_id='ingest_sap_orders',
        bash_command='echo "ADF + COPY INTO: SAP orders CSV → Bronze via Snowpipe"',
    )

    ingest_vendor_data = BashOperator(
        task_id='ingest_vendor_data',
        bash_command='echo "SSIS + BODS: Vendor master → Bronze (C# validation)"',
    )

    ingest_equipment = BashOperator(
        task_id='ingest_equipment',
        bash_command='echo "Pandas read_excel: Equipment registry → Bronze"',
    )

    # dbt staging: 5 Bronze views
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt run --models staging --profiles-dir /opt/airflow/config '
            '|| echo "dbt staging failed — check connection"'
        ),
    )

    # ── Bronze Gate (Branch) ──
    bronze_gate = BranchPythonOperator(
        task_id='bronze_gate',
        python_callable=bronze_gate_check,
    )

    bronze_gate_passed = EmptyOperator(task_id='bronze_gate_passed')
    bronze_gate_failed = EmptyOperator(task_id='bronze_gate_failed')

    quarantine_bronze = PythonOperator(
        task_id='quarantine_bronze',
        python_callable=quarantine_failed_records,
        op_kwargs={'layer': 'bronze'},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    email_bronze_fail = EmailOperator(
        task_id='email_bronze_fail',
        to='pechnarai.jakapong@gmail.com',
        subject='PETROVA Bronze Gate FAIL — Schema Mismatch',
        html_content='<p>Bronze Gate validation failed. Records quarantined.</p>',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ═══════════════════════════════════════════
    # STAGE 2: SILVER (Cleaned + Validated)
    # ═══════════════════════════════════════════

    # dbt intermediate: 5 Silver models (incremental)
    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt run --models intermediate --profiles-dir /opt/airflow/config'
        ),
    )

    dbt_test_intermediate = BashOperator(
        task_id='dbt_test_intermediate',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt test --models intermediate --profiles-dir /opt/airflow/config'
        ),
    )

    # ── Silver Gate (Branch) ──
    silver_gate = BranchPythonOperator(
        task_id='silver_gate',
        python_callable=silver_gate_check,
    )

    silver_gate_passed = EmptyOperator(task_id='silver_gate_passed')
    silver_gate_failed = EmptyOperator(task_id='silver_gate_failed')

    quarantine_silver = PythonOperator(
        task_id='quarantine_silver',
        python_callable=quarantine_failed_records,
        op_kwargs={'layer': 'silver'},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    email_silver_fail = EmailOperator(
        task_id='email_silver_fail',
        to='pechnarai.jakapong@gmail.com',
        subject='PETROVA Silver Gate FAIL — Quality Check Failed',
        html_content='<p>Silver Gate quality validation failed. Check quarantine table.</p>',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ═══════════════════════════════════════════
    # STAGE 3: GOLD (Business-Ready + Immutable)
    # ═══════════════════════════════════════════

    # Layer B: Cache Gold snapshot BEFORE rebuild
    cache_gold = PythonOperator(
        task_id='cache_gold_snapshot',
        python_callable=cache_gold_snapshot,
    )

    # dbt marts: 6 Gold models (3 facts + 3 dims)
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt run --models marts --profiles-dir /opt/airflow/config'
        ),
    )

    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt test --models marts --profiles-dir /opt/airflow/config'
        ),
    )

    # ── Gold Gate SPC (Branch) ──
    gold_gate_spc = BranchPythonOperator(
        task_id='gold_gate_spc',
        python_callable=gold_gate_spc_check,
    )

    gold_gate_passed = EmptyOperator(task_id='gold_gate_passed')
    gold_gate_failed = EmptyOperator(task_id='gold_gate_failed')

    # Gold failure → CRITICAL alert (PagerDuty + Email)
    alert_gold_critical = BashOperator(
        task_id='alert_gold_critical',
        bash_command=(
            'echo "CRITICAL: Gold Gate SPC alert — '
            'composite severity CRITICAL (UNSTABLE + NOISY). '
            'Paging on-call via PagerDuty. '
            'BI dashboards falling back to cached Gold snapshot."'
        ),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        # In production:
        # PagerDutyOperator(
        #     pagerduty_conn_id='pagerduty_default',
        #     summary='Gold Gate SPC CRITICAL',
        #     severity='critical',
        # )
    )

    email_gold_fail = EmailOperator(
        task_id='email_gold_fail',
        to='pechnarai.jakapong@gmail.com',
        subject='PETROVA Gold Gate SPC CRITICAL — Composite Alert',
        html_content=(
            '<p><b>CRITICAL:</b> Gold Gate SPC triggered composite severity.</p>'
            '<p>UNSTABLE + NOISY detected. BI serving cached Gold snapshot.</p>'
            '<p>Check: <code>SELECT * FROM gold.fct_sensor_alerts '
            'WHERE alert_severity = \'CRITICAL\' ORDER BY kpi_date DESC</code></p>'
        ),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ═══════════════════════════════════════════
    # STAGE 4: POST-PROCESSING & NOTIFICATION
    # ═══════════════════════════════════════════

    # dbt docs generate (for dbt Cloud Studio lineage)
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt docs generate --profiles-dir /opt/airflow/config'
        ),
    )

    # dbt snapshot (SCD2 history tracking)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt snapshot --profiles-dir /opt/airflow/config'
        ),
    )

    notify_success = EmailOperator(
        task_id='notify_success',
        to='pechnarai.jakapong@gmail.com',
        subject='PETROVA Pipeline SUCCESS — All Gates Passed',
        html_content=(
            '<p>PETROVA pipeline completed successfully.</p>'
            '<p>Bronze ✓ | Silver ✓ | Gold ✓ | SPC ✓</p>'
        ),
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,  # Layer A: runs even if upstream partial fail
    )

    # ═══════════════════════════════════════════════
    # TASK DEPENDENCIES (with failover paths)
    # ═══════════════════════════════════════════════

    # Stage 1: Parallel ingestion → staging → Bronze Gate
    start >> [ingest_sensor_data, ingest_sap_orders, ingest_vendor_data, ingest_equipment]
    [ingest_sensor_data, ingest_sap_orders, ingest_vendor_data, ingest_equipment] >> dbt_run_staging
    dbt_run_staging >> bronze_gate

    # Bronze Gate: pass → Silver | fail → quarantine + email + end
    bronze_gate >> [bronze_gate_passed, bronze_gate_failed]
    bronze_gate_failed >> quarantine_bronze >> email_bronze_fail >> end
    bronze_gate_passed >> dbt_run_intermediate >> dbt_test_intermediate >> silver_gate

    # Silver Gate: pass → Gold | fail → quarantine + email + end
    silver_gate >> [silver_gate_passed, silver_gate_failed]
    silver_gate_failed >> quarantine_silver >> email_silver_fail >> end

    # Gold: cache snapshot FIRST, then rebuild, then test, then SPC gate
    silver_gate_passed >> cache_gold >> dbt_run_marts >> dbt_test_marts >> gold_gate_spc

    # Gold Gate: pass → post-processing | fail → CRITICAL alert + email + end
    gold_gate_spc >> [gold_gate_passed, gold_gate_failed]
    gold_gate_failed >> alert_gold_critical >> email_gold_fail >> end
    gold_gate_passed >> [dbt_docs, dbt_snapshot] >> notify_success >> end
