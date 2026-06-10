# 🥈 Silver Layer — 10 Transformations (Bronze → Silver)
_Where raw Bronze becomes clean, validated, enriched data. Runs in dbt Cloud IDE / Databricks notebooks / Snowsight (non-CLI)._

## Bronze → Silver per source
| # | Source | Type | Bronze tool | Silver transformation | Silver tool |
|---|---|---|---|---|---|
| 1 | IoT Sensors | Dynamic | Kafka + Spark Streaming | clean nulls, standardise units, dedupe | Spark |
| 2 | App Logs | Dynamic | Fluentd/Logstash → Kafka | parse → schema, extract error codes | Spark / ELK |
| 3 | Equipment Master | Static | Airflow + JDBC (SAP) | cleansing, **SCD2** history | dbt / SQL |
| 4 | Maintenance | Semi | Airflow + API | normalise, join equipment_id | dbt / Spark |
| 5 | Weather | Semi | Airflow (Python) | timezone convert, aggregate | Spark / SQL |
| 6 | SCADA / PLC | Dynamic | OPC-UA → Kafka | filter noise, align timestamps | Spark Streaming |
| 7 | Transaction DB | Dynamic | Debezium CDC + Kafka | dedupe (CDC), business rules | Spark / dbt |
| 8 | Reference Data | Static | CSV / S3 | validate, PK/FK constraints | dbt |
| 9 | Clickstream | Dynamic | Kafka / Kinesis | sessionise, remove bots | Spark / Flink |
| 10 | Legacy File | Semi | Airflow + SFTP | schema validate, column map | Spark |

## The 10 Silver transformations (SQL)

**1. Deduplication** — keep latest per key
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY equipment_id, timestamp ORDER BY updated_at DESC) AS rn
  FROM bronze_sensor
) t WHERE rn = 1;
```

**2. Data cleansing** — fix bad values ⚠️ *don't impute 0 on sensor signals — NULL/flag instead*
```sql
SELECT equipment_id,
       temperature,                                  -- keep NULL, don't COALESCE to 0
       CASE WHEN pressure < 0 THEN NULL ELSE pressure END AS pressure
FROM bronze_sensor;
```

**3. Schema enforcement** — consistent types
```sql
SELECT equipment_id::VARCHAR, timestamp::TIMESTAMP, temperature::NUMERIC(10,2)
FROM bronze_sensor;
```

**4. Standardisation** — units / formats
```sql
SELECT equipment_id, temperature_celsius,
       (temperature_celsius * 9/5) + 32 AS temperature_fahrenheit
FROM bronze_sensor;
```

**5. Enrichment** — join static + dynamic (the materialization)
```sql
SELECT s.*, e.equipment_type, e.max_pressure
FROM silver_sensor s JOIN silver_equipment e ON s.equipment_id = e.equipment_id;
```

**6. Aggregation** — ⚠️ *usually a GOLD concern, shown here for completeness*
```sql
SELECT equipment_id, DATE_TRUNC('hour', timestamp) AS hour,
       AVG(temperature) AS avg_temp, MAX(pressure) AS max_pressure
FROM silver_sensor GROUP BY equipment_id, hour;
```

**7. Filtering** — remove noise
```sql
SELECT * FROM bronze_sensor WHERE temperature IS NOT NULL AND pressure > 0;
```

**8. CDC handling (UPSERT)** — Postgres `ON CONFLICT` / Snowflake-Databricks = `MERGE`
```sql
INSERT INTO silver_equipment
SELECT * FROM bronze_equipment
ON CONFLICT (equipment_id) DO UPDATE
  SET equipment_type = EXCLUDED.equipment_type, updated_at = NOW();
```

**9. Validation** — business rules (flag, don't just drop)
```sql
SELECT *, CASE WHEN pressure BETWEEN 0 AND 5000 THEN 'VALID' ELSE 'INVALID' END AS validation_status
FROM silver_sensor;
```

**10. Normalisation** — flatten JSON
```sql
SELECT data->>'equipment_id' AS equipment_id,
       (data->>'temperature')::NUMERIC AS temperature,
       (data->>'pressure')::NUMERIC AS pressure
FROM bronze_json;
```

## 🧱 Layering (keep crisp)
- **Bronze** = raw, immutable, append-only
- **Silver** = clean · dedupe · enforce schema · standardise · validate · enrich · SCD2/CDC
- **Gold** = aggregate KPIs · business marts · SPC alerts (immutable)

## 🎤 Interview line
> *"In Silver I dedupe with ROW_NUMBER, enforce schema, standardise units, validate against business rules (flagging not dropping), handle SCD2/CDC with MERGE, and enrich the dynamic signal with static context. I keep aggregation in Gold so the layers stay clean — and I never silently impute zeros on sensor data."*
