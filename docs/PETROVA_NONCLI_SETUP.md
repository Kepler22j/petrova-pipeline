# 🖱️ PETROVA — Non-CLI (UI) Setup: the WHOLE pipeline, click-by-click
_Stand up the full pipeline using each tool's web UI — no command line. (CLI equivalents noted at the end.)_

**Flow:** Sources → ingest → **Snowflake Bronze** → **dbt Silver/Gold** → **Airflow orchestrate** → **Streamlit dashboard**

---

## Phase 1 — Snowflake (Snowsight UI)
| Step | UI Navigation (non-CLI) |
|---|---|
| Create warehouse | **Admin → Warehouses → + Warehouse** → name `PETROVA_DEV_WH`, size X-Small, auto-suspend 60s |
| Create database | **Data → Databases → + Database** → `PETROVA_DEV` |
| Create schemas | open `PETROVA_DEV` → **+ Schema** → `BRONZE`, `SILVER`, `GOLD` |
| Create role | **Admin → Users & Roles → Roles → + Role** → `PETROVA_ENGINEER` → grant on DB + WH |
| Load sample data | **Data → Databases → BRONZE → (table) → Load Data** → upload CSV (wizard — no CLI) |

## Phase 2 — Ingestion (Azure Data Factory Studio UI)
| Step | UI Navigation |
|---|---|
| Linked services | **Manage → Linked services → + New** → Azure Blob + Snowflake (creds from Key Vault) |
| Build pipeline | **Author → + → Pipeline → Copy data** activity → source = Blob/flat-file, sink = Snowflake `BRONZE` |
| Run it | **Add trigger → Trigger now** (or schedule) → **Monitor** tab to watch |
| *(Alt, simpler)* | Snowsight **Data → Load Data** wizard instead of ADF |

## Phase 3 — Transformation (dbt Cloud UI)
| Step | UI Navigation |
|---|---|
| Connect | dbt Cloud → **Account settings → Connection → Snowflake** (account, WH, DB, role) |
| Run models | **Develop → Cloud IDE →** click **`dbt build`** (runs Bronze→Silver→Gold + tests = your 3-gate) |
| Schedule | **Deploy → Jobs → + Create job** → command `dbt build` → set schedule |
| *(Alt)* | Databricks UI: **Workspace → Notebook → Run all** |

## Phase 4 — Orchestration (Airflow UI)
| Step | UI Navigation |
|---|---|
| Enable DAG | Airflow UI → **DAGs** → toggle ON `petrova_validated_pipeline` |
| Trigger | click **▶ Trigger DAG** → **Graph** view to watch tasks (green/red) |
| Investigate | click a task → **Logs** (alerts fire on failure / SLA miss) |

## Phase 5 — Dashboard (Streamlit Community Cloud UI)
| Step | UI Navigation |
|---|---|
| Deploy | **share.streamlit.io → New app** → pick GitHub repo + `streamlit_app.py` → **Deploy** |
| Secrets | app → **⚙ Settings → Secrets** → paste Snowflake creds (TOML) |
| Verify | open the app URL → Gold metrics + SPC alerts render |

## Phase 6 — Verify the whole pipeline
1. ADF/Snowsight shows Bronze loaded → 2. dbt shows Silver/Gold built + tests passed → 3. Airflow DAG green → 4. Streamlit dashboard live.

---

## CLI equivalents (for reference / automation)
| UI step | CLI |
|---|---|
| Snowflake objects | `snow sql -f snowflake/ddl/*.sql` |
| dbt build | `dbt build` |
| Airflow trigger | `airflow dags trigger petrova_validated_pipeline` |
| Streamlit (local) | `streamlit run streamlit_app.py` |

## 🎤 Interview line
> "I can stand up the whole pipeline from the UIs — Snowsight, ADF Studio, dbt Cloud, Airflow, Streamlit — which is how I onboard analysts and non-engineers; and from CLI/code for automation and CI/CD. Same pipeline, two interfaces."
