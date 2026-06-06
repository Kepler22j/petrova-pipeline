# PETROVA — Project Status

_Snapshot of current state and next steps for the pipeline._

## Current state
- **Data quality (3-gate)** — `macros/three_gate_validation.sql` provides `silver_quality_flag`,
  `silver_is_valid`, `bronze_schema_gate`, `gold_gate_spc`. Wired into `int_sensor_cleaned`
  (FAIL / WARN / PASS).
- **SPC alert engine** — `models/marts/fct_sensor_alerts.sql`: 6 categories from STDDEV + LAG +
  THRESHOLD with composite severity (OK / WARNING / CRITICAL).
- **Orchestration** — `airflow/dags/petrova_validated_pipeline.py`: Bronze/Silver/Gold gates query
  Snowflake via `SnowflakeHook` with a graceful demo-mode fallback (no warehouse required to run).
  SLA protection: Zero-Copy Clone cache + `trigger_rule='all_done'`.
- **Connections (config-as-code)** — `dbt/profiles.yml.example` + `.env.example`: `dev` (PETROVA_DEV),
  `prod` (PETROVA_PROD), `databricks` targets, all from env vars. No secrets in repo.
- **Demo data** — `scripts/generate_gold_demo.py` produces `data/gold/*.csv` (synthetic offshore
  telemetry with an injected K-201 bearing-degradation fault). Mirrors the dbt SPC logic.
- **Dashboard** — `streamlit_app.py`: 3-tier data source (live Snowflake → Gold CSV → synthetic)
  with a sidebar source badge.

## Run locally
```bash
py scripts/generate_gold_demo.py          # generate data/gold/*.csv
py -m streamlit run streamlit_app.py      # http://localhost:8501  (Demo mode)
# live mode: set SNOWFLAKE_* in .env + pip install snowflake-connector-python[pandas]
dbt run --target dev --profiles-dir dbt   # needs Snowflake creds
```

## Next steps
- [ ] Deploy/verify Streamlit Cloud (main file: `streamlit_app.py`).
- [ ] SPC: replace fixed stddev thresholds (5 / 25) with per-sensor control limits.
- [ ] Wire the 3-gate macro into `int_orders_validated` for parity with `int_sensor_cleaned`.
- [ ] Strengthen `tests/e2e` assertions (currently permissive).
- [ ] Connect Airflow gate queries to a live warehouse (currently demo-fallback).

## Notes
- `data/gold/*.csv` is committed (small) so the dashboard renders without a warehouse;
  bulk/landing data and Parquet are gitignored.
