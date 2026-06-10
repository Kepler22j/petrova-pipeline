# 🛢️ Static vs Dynamic Ingestion → Bronze (Non-CLI / UI)
_APM platform (offshore rigs). 5 sources, each set up from the web UI, into the medallion Bronze layer._

## Static vs Dynamic (the two lanes)
| | Static (reference / context) | Dynamic (operational / signal) |
|---|---|---|
| Example | Equipment Master `{equipment_id, rig_id, type, manufacturer, max_pressure}` | Sensor reading `{timestamp, equipment_id, temperature, pressure, vibration}` |
| Changes | rarely | every second |
| Used for | lookup / enrichment | alerts / anomaly detection |
| Becomes | dimensions (SCD2) | facts (append-only) |

## The 5 sources → Bronze (Non-CLI / UI setup)
| # | Source | Type | Tool | Non-CLI (UI) setup |
|---|---|---|---|---|
| 1 | **IoT Sensors** | Dynamic (stream) | Event Hub → Auto Loader / Snowpipe | Azure Portal → Event Hubs → + Namespace + Hub; Databricks → Notebook → `readStream` (cloudFiles) → write `bronze.sensors` |
| 2 | **Incident Logs** | Dynamic | File / ELK → ADF | ADF Studio → Author → Copy data (source = log files/Blob, sink = `bronze.logs`) → trigger |
| 3 | **Equipment Master** | Static | SAP → ADF batch | ADF Studio → Manage → Linked service (SAP) → Copy → `bronze.equipment_master` (schedule daily) |
| 4 | **Maintenance (CMMS)** | Semi-dynamic | REST API → ADF | ADF Studio → Linked service (REST) → Copy (API → `bronze.maintenance`) → schedule |
| 5 | **Weather** | Semi-dynamic (batch) | Weather API → ADF | ADF Studio → REST/HTTP linked service → Copy hourly → `bronze.weather` |

## Bronze layer (raw · immutable · source-aligned · append-only)
```
bronze/
  ├── sensors/            (dynamic, streamed)
  ├── logs/               (dynamic)
  ├── equipment_master/   (static)
  ├── maintenance/        (semi-dynamic)
  └── weather/            (semi-dynamic)
```
No business logic. Raw format preserved. Partition by ingest date + source.

## Combine: Static + Dynamic in Silver/Gold (the materialization)
```
Dynamic (sensor)  +  Static (equipment_master)  ->  enriched  ->  anomaly detection
```
```sql
-- Silver/Gold: enrich the live signal with static context, then flag anomalies
SELECT
    s.timestamp,
    s.equipment_id,
    e.rig_id,
    e.equipment_type,
    s.pressure,
    e.max_pressure,                                   -- static threshold
    CASE WHEN s.pressure > e.max_pressure             -- anomaly rule from static data
         THEN 'CRITICAL' ELSE 'OK' END AS pressure_status
FROM   bronze.sensors        s
JOIN   bronze.equipment_master e
  ON   s.equipment_id = e.equipment_id;
```

## 📚 10-source ingestion reference (right tool per source — the senior skill)
| # | Source | Type | Example | Recommended tools | Why / best compromise |
|---|---|---|---|---|---|
| 1 | IoT Sensors (rig/machines) | Dynamic | temp, pressure | Kafka + Spark Structured Streaming | high-throughput + near-real-time |
| 2 | Application Logs | Dynamic | errors, events | Fluentd/Logstash + Kafka | reliable shipping + buffering |
| 3 | Equipment Master (ERP/SAP) | Static | metadata | SAP BODS / Airflow + JDBC | batch = simple, consistent, low cost |
| 4 | Maintenance (CMMS) | Semi | work orders | Airflow + REST API | controlled ingestion + retry |
| 5 | Weather API | Semi (batch) | temp, humidity | Airflow + Python (requests) | scheduled polling, cost-efficient |
| 6 | SCADA / PLC | Dynamic | industrial signals | **OPC-UA + Kafka** | industrial standard + decoupled |
| 7 | Transaction DB (OLTP) | Dynamic | orders, payments | **Debezium (CDC) + Kafka** | near-real-time, exactly-once |
| 8 | Reference Data | Static | country, unit | CSV + Git / S3 | simple, version-controlled |
| 9 | Clickstream / User Events | Dynamic | page views | Kafka/Kinesis + Spark/Flink | high-volume event streaming |
| 10 | Legacy File Drops (SFTP) | Semi | daily reports | Airflow + SFTP + Spark batch | easy legacy integration |

> 🎤 **Senior signal:** *"I match the ingestion tool to the source's velocity and cost profile — streaming (Kafka/OPC-UA/Debezium) only where real-time matters, batch (Airflow/SAP BODS) where it doesn't. The skill is the compromise: don't stream a daily weather feed, don't batch a safety-critical sensor."*

## 🧠 Senior guards (name these)
- **Watermark** — late sensor events arrive out of order; watermark bounds them.
- **CDC** — capture only *changes* to equipment master (not full reloads).
- **Idempotent MERGE** — re-running ingestion can't duplicate (key + row_hash).
- **Exactly-once** — Event Hub + checkpoint + idempotent write.

## 🎤 Interview answer
> *"I ingest in two lanes into an immutable Bronze: dynamic sensor signals via Event Hub + Auto Loader (append-only, watermarked), and static equipment master via ADF batch (SCD2, CDC). I materialize value by joining the live signal to the static context in Silver/Gold — e.g., comparing live pressure to the equipment's rated max_pressure to raise a CRITICAL anomaly. Idempotent merges keep re-runs safe."*
