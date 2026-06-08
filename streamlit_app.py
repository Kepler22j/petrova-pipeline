"""
PETROVA Pipeline — Live Dashboard
══════════════════════════════════
Production Data Platform Monitor | Built by Jay Pechnarai
github.com/Kepler22j/petrova-pipeline
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random

# ═══════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════
st.set_page_config(
    page_title="PETROVA Pipeline Dashboard",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Seed for reproducible demo data
np.random.seed(42)

# ═══════════════════════════════════════
# GENERATE DEMO DATA
# ═══════════════════════════════════════
import os
from pathlib import Path
_GOLD_DIR = Path(__file__).parent / "data" / "gold"


def _snowflake_gold(table):
    """Tier 1 — live Gold table from Snowflake if creds + driver present, else None."""
    if not os.environ.get("SNOWFLAKE_ACCOUNT"):
        return None
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "PETROVA_READER"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "PETROVA_PROD_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "PETROVA_PROD"),
        )
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM PETROVA_PROD.GOLD.{table}")
        out = cur.fetch_pandas_all()
        conn.close()
        out.columns = [c.lower() for c in out.columns]
        return out
    except Exception:
        return None


def data_source_mode():
    """Cheap badge for the sidebar — Live / Demo CSV / Synthetic."""
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        try:
            import snowflake.connector  # noqa: F401
            return "🟢 Live (Snowflake)"
        except Exception:
            pass
    if (_GOLD_DIR / "fct_daily_sensor_kpi.csv").exists():
        return "🟡 Demo (Gold CSV)"
    return "⚪ Synthetic (generated)"


@st.cache_data
def generate_sensor_data():
    # Tier 1 Snowflake -> Tier 2 generated Gold CSV -> Tier 3 synthesize
    live = _snowflake_gold("FCT_DAILY_SENSOR_KPI")
    if live is not None:
        return live.rename(columns={"kpi_date": "date"})
    _gold = _GOLD_DIR / "fct_daily_sensor_kpi.csv"
    if _gold.exists():
        return pd.read_csv(_gold, parse_dates=["kpi_date"]).rename(columns={"kpi_date": "date"})
    dates = pd.date_range("2025-11-01", "2025-12-31", freq="D")
    sensors = ["S001-Vibration", "S002-Temperature", "S003-Pressure", "S004-Flow", "S005-RPM"]
    rows = []
    for d in dates:
        for s in sensors:
            base = {"S001-Vibration": 12, "S002-Temperature": 75, "S003-Pressure": 4.5, "S004-Flow": 120, "S005-RPM": 3200}[s]
            noise = np.random.normal(0, base * 0.08)
            spike = np.random.choice([0, base * 0.4], p=[0.95, 0.05])
            avg_val = base + noise + spike
            std_val = abs(np.random.normal(base * 0.05, base * 0.02))
            rows.append({
                "date": d, "sensor_id": s.split("-")[0], "sensor_name": s,
                "avg_reading": round(avg_val, 2), "stddev_reading": round(std_val, 2),
                "min_reading": round(avg_val - std_val * 2, 2),
                "max_reading": round(avg_val + std_val * 2 + spike, 2),
                "total_readings": np.random.randint(800, 1200),
            })
    return pd.DataFrame(rows)

@st.cache_data
def generate_alert_data(df):
    _ren = {"kpi_date": "date", "stability_level": "stability",
            "coefficient_of_variation": "cv", "alert_severity": "severity"}
    live = _snowflake_gold("FCT_SENSOR_ALERTS")
    if live is not None:
        return live.rename(columns=_ren)
    _gold = _GOLD_DIR / "fct_sensor_alerts.csv"
    if _gold.exists():
        return pd.read_csv(_gold, parse_dates=["kpi_date"]).rename(columns=_ren)
    alerts = []
    for _, r in df.iterrows():
        stability = "STABLE" if r["stddev_reading"] < 5 else ("NORMAL" if r["stddev_reading"] <= 25 else "UNSTABLE")
        cv = r["stddev_reading"] / max(abs(r["avg_reading"]), 0.01)
        signal = "CLEAN" if cv <= 0.3 else ("NOISY" if cv <= 0.5 else "VERY_NOISY")
        has_outlier = r["max_reading"] > r["avg_reading"] + 2 * r["stddev_reading"]
        severity = "CRITICAL" if (stability == "UNSTABLE" and signal != "CLEAN") else ("WARNING" if stability == "UNSTABLE" or has_outlier else "OK")
        alerts.append({
            "date": r["date"], "sensor_id": r["sensor_id"], "sensor_name": r["sensor_name"],
            "stability": stability, "signal_quality": signal,
            "has_outlier": has_outlier, "cv": round(cv, 4), "severity": severity,
        })
    return pd.DataFrame(alerts)

@st.cache_data
def generate_pipeline_runs():
    dates = pd.date_range("2025-11-01", "2025-12-31", freq="D")
    runs = []
    for d in dates:
        bronze_ok = np.random.choice([True, True, True, True, True, True, True, True, True, False])
        silver_ok = bronze_ok and np.random.choice([True, True, True, True, True, True, True, True, True, False])
        gold_ok = silver_ok and np.random.choice([True, True, True, True, True, True, True, True, True, True, False])
        duration = np.random.randint(12, 45) if gold_ok else np.random.randint(5, 20)
        runs.append({
            "date": d, "bronze_gate": "PASS" if bronze_ok else "FAIL",
            "silver_gate": "PASS" if silver_ok else ("FAIL" if bronze_ok else "SKIP"),
            "gold_gate": "PASS" if gold_ok else ("FAIL" if silver_ok else "SKIP"),
            "status": "SUCCESS" if gold_ok else "PARTIAL",
            "duration_min": duration,
            "records_processed": np.random.randint(380000, 420000) if bronze_ok else 0,
            "quarantined": np.random.randint(0, 50) if not gold_ok else 0,
        })
    return pd.DataFrame(runs)

sensor_df = generate_sensor_data()
alert_df = generate_alert_data(sensor_df)
pipeline_df = generate_pipeline_runs()

# ═══════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════
st.sidebar.image("https://raw.githubusercontent.com/Kepler22j/petrova-pipeline/main/docs/logo.png", width=60) if False else None
st.sidebar.title("PETROVA Pipeline")
st.sidebar.caption("Production Data Platform Monitor")
st.sidebar.caption(f"Data source: {data_source_mode()}")
st.sidebar.markdown("---")

page = st.sidebar.radio("Navigate", ["Pipeline Overview", "Data Quality & SPC", "Architecture"])
st.sidebar.markdown("---")
st.sidebar.markdown("**Built by Jay Pechnarai**")
st.sidebar.caption("Senior Data Platform Engineer · Data Architect")
st.sidebar.markdown("[GitHub](https://github.com/Kepler22j/petrova-pipeline) · [LinkedIn](https://linkedin.com/in/jakapong-pechnarai-4001986b)")
st.sidebar.caption("Snowflake · Databricks · dbt · Airflow")

# ═══════════════════════════════════════
# PAGE 1: PIPELINE OVERVIEW
# ═══════════════════════════════════════
if page == "Pipeline Overview":
    st.title("Pipeline Overview")
    st.caption("PETROVA — 400K+ Records/Day · 3-Gate Validation · SLA Protection")

    # KPI Cards
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    total_records = pipeline_df["records_processed"].sum()
    success_rate = (pipeline_df["status"] == "SUCCESS").mean() * 100
    avg_duration = pipeline_df["duration_min"].mean()
    total_quarantined = pipeline_df["quarantined"].sum()
    total_alerts = (alert_df["severity"] != "OK").sum()
    critical_count = (alert_df["severity"] == "CRITICAL").sum()

    col1.metric("Total Records", f"{total_records:,.0f}")
    col2.metric("Success Rate", f"{success_rate:.1f}%")
    col3.metric("Avg Duration", f"{avg_duration:.0f} min")
    col4.metric("Quarantined", f"{total_quarantined:,}")
    col5.metric("Alerts", f"{total_alerts:,}")
    col6.metric("Critical", f"{critical_count}", delta=f"{critical_count}" if critical_count > 0 else None, delta_color="inverse")

    st.markdown("---")

    # Pipeline runs chart
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Daily Pipeline Runs")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=pipeline_df["date"], y=pipeline_df["records_processed"],
                             marker_color=pipeline_df["status"].map({"SUCCESS": "#27AE60", "PARTIAL": "#F39C12"}),
                             name="Records Processed"))
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                         xaxis_title="Date", yaxis_title="Records",
                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Gate Pass Rates")
        gates = {
            "Bronze Gate": (pipeline_df["bronze_gate"] == "PASS").mean() * 100,
            "Silver Gate": (pipeline_df["silver_gate"] == "PASS").mean() * 100,
            "Gold Gate": (pipeline_df["gold_gate"] == "PASS").mean() * 100,
        }
        fig2 = go.Figure(go.Bar(
            x=list(gates.values()), y=list(gates.keys()),
            orientation="h",
            marker_color=["#2E75B6", "#27AE60", "#F1C40F"],
            text=[f"{v:.1f}%" for v in gates.values()],
            textposition="auto"
        ))
        fig2.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                          xaxis=dict(range=[0, 100]), plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)

    # Duration trend
    st.subheader("Pipeline Duration Trend")
    fig3 = px.line(pipeline_df, x="date", y="duration_min", color_discrete_sequence=["#2E75B6"])
    fig3.update_layout(height=250, margin=dict(l=20, r=20, t=10, b=20),
                      xaxis_title="Date", yaxis_title="Minutes",
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig3, use_container_width=True)

    # Recent runs table
    st.subheader("Recent Pipeline Runs")
    recent = pipeline_df.tail(10).sort_values("date", ascending=False)
    st.dataframe(recent[["date", "status", "bronze_gate", "silver_gate", "gold_gate", "records_processed", "duration_min", "quarantined"]],
                 use_container_width=True, hide_index=True)

# ═══════════════════════════════════════
# PAGE 2: DATA QUALITY & SPC
# ═══════════════════════════════════════
elif page == "Data Quality & SPC":
    st.title("Data Quality & SPC Alerts")
    st.caption("Statistical Process Control — 6 Alert Categories from 3 Primitives")

    # Alert severity distribution
    col1, col2, col3 = st.columns(3)
    ok_count = (alert_df["severity"] == "OK").sum()
    warn_count = (alert_df["severity"] == "WARNING").sum()
    crit_count = (alert_df["severity"] == "CRITICAL").sum()
    col1.metric("OK", ok_count, delta="All clear", delta_color="normal")
    col2.metric("WARNING", warn_count, delta="Single trigger", delta_color="off")
    col3.metric("CRITICAL", crit_count, delta="Multiple triggers", delta_color="inverse")

    st.markdown("---")

    # Severity over time
    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.subheader("Alert Severity Over Time")
        sev_daily = alert_df.groupby(["date", "severity"]).size().reset_index(name="count")
        fig = px.bar(sev_daily, x="date", y="count", color="severity",
                     color_discrete_map={"OK": "#27AE60", "WARNING": "#F39C12", "CRITICAL": "#E74C3C"},
                     barmode="stack")
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                         plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Severity Distribution")
        sev_counts = alert_df["severity"].value_counts()
        fig2 = px.pie(values=sev_counts.values, names=sev_counts.index,
                      color=sev_counts.index,
                      color_discrete_map={"OK": "#27AE60", "WARNING": "#F39C12", "CRITICAL": "#E74C3C"})
        fig2.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    # Sensor selector
    st.subheader("Sensor Deep Dive")
    selected_sensor = st.selectbox("Select Sensor", sensor_df["sensor_name"].unique())
    sensor_data = sensor_df[sensor_df["sensor_name"] == selected_sensor]

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Average Reading with StdDev Band**")
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=sensor_data["date"], y=sensor_data["avg_reading"],
                                  mode="lines", name="Avg", line=dict(color="#2E75B6")))
        fig3.add_trace(go.Scatter(x=sensor_data["date"],
                                  y=sensor_data["avg_reading"] + sensor_data["stddev_reading"],
                                  mode="lines", name="Upper Band", line=dict(color="#E74C3C", dash="dash")))
        fig3.add_trace(go.Scatter(x=sensor_data["date"],
                                  y=sensor_data["avg_reading"] - sensor_data["stddev_reading"],
                                  mode="lines", name="Lower Band", line=dict(color="#E74C3C", dash="dash"),
                                  fill="tonexty", fillcolor="rgba(231,76,60,0.1)"))
        fig3.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20),
                          plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig3, use_container_width=True)

    with col_b:
        st.markdown("**StdDev Trend (Volatility)**")
        fig4 = px.line(sensor_data, x="date", y="stddev_reading", color_discrete_sequence=["#F39C12"])
        fig4.update_layout(height=300, margin=dict(l=20, r=20, t=10, b=20),
                          plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig4, use_container_width=True)

    # SPC Primitives explanation
    st.markdown("---")
    st.subheader("SPC Engine: 3 Primitives → 6 Alerts")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**STDDEV** (Spread)")
        st.markdown("- Alert 1: Stability Level\n- Alert 2: Signal Quality (CV)\n- Alert 3: Outlier Detection\n- Alert 4: Range Violation")
    with col2:
        st.markdown("**LAG** (Change)")
        st.markdown("- Alert 5: Volatility Trend\n- Alert 6: Spike Detection")
    with col3:
        st.markdown("**THRESHOLD** (Limits)")
        st.markdown("- All 6 alerts use configurable thresholds\n- Composite: UNSTABLE + NOISY = CRITICAL")

    # Alert detail table
    st.subheader("Recent Alerts (Non-OK)")
    non_ok = alert_df[alert_df["severity"] != "OK"].sort_values("date", ascending=False).head(20)
    st.dataframe(non_ok[["date", "sensor_name", "severity", "stability", "signal_quality", "has_outlier", "cv"]],
                 use_container_width=True, hide_index=True)

# ═══════════════════════════════════════
# PAGE 3: ARCHITECTURE
# ═══════════════════════════════════════
elif page == "Architecture":
    st.title("PETROVA Architecture")
    st.caption("9-Layer Production Data Platform | 400K+ Records/Day")

    st.markdown("---")

    # ── Final-result visual report: architecture image + presentation deck ──
    _assets = Path(__file__).parent / "assets"
    _arch = _assets / "petrova_architecture.png"
    if _arch.exists():
        st.image(str(_arch), use_container_width=True, caption="PETROVA — end-to-end architecture")
    _deck = sorted((_assets / "deck").glob("Slide*.PNG"),
                   key=lambda p: int("".join(c for c in p.stem if c.isdigit()) or 0))
    if _deck:
        st.subheader("📊 Presentation Deck")
        n = st.slider("Slide", 1, len(_deck), 1) if len(_deck) > 1 else 1
        st.image(str(_deck[n - 1]), use_container_width=True)
    _pdf = _assets / "PETROVA_Presentation.pdf"
    if _pdf.exists():
        st.download_button("⬇️  Download full deck (PDF)", _pdf.read_bytes(),
                           file_name="PETROVA_Presentation.pdf", mime="application/pdf")

    st.markdown("---")

    # Architecture overview
    col1, col2 = st.columns([1, 1])
    with col1:
        st.subheader("9-Layer Stack")
        layers = [
            ("Layer 0", "Data Sources", "IoT Sensors, SAP BODS, Equipment Registry, SQL Server, Manual/Excel"),
            ("Layer 1", "Ingestion", "Azure Data Factory, SSIS, Snowpipe (Auto Loader)"),
            ("Layer 2", "Medallion", "Bronze (Raw) → Silver (Cleaned, SCD2) → Gold (KPIs, Immutable)"),
            ("Layer 3", "Compute", "Databricks (PySpark) + Snowflake (SQL) — dual platform"),
            ("Layer 4", "Orchestration", "Apache Airflow + ADF + Databricks Workflows"),
            ("Layer 5", "Analytics", "Power BI, Streamlit, dbt Cloud Studio, Jupyter"),
            ("Layer 6", "Monitoring", "PagerDuty, Azure Monitor, Grafana, Snowflake Alerts"),
            ("Layer 7", "Security", "4-Role RBAC, AES-256, TLS 1.2+, Unity Catalog"),
            ("Layer 8", "Local Dev", "Docker Compose (PostgreSQL, Airflow, Spark, MinIO)"),
        ]
        for num, name, desc in layers:
            st.markdown(f"**{num}: {name}** — {desc}")

    with col2:
        st.subheader("Key Metrics")
        metrics = {
            "Records/Day": "400K+ (Design Target)",
            "Pipeline SLA": "99.5% (Designed)",
            "Validation Gates": "3 (Schema → Business → SPC)",
            "Alert Categories": "6 (from 3 primitives)",
            "dbt Models": "16 (5 staging + 5 intermediate + 6 marts)",
            "Automated Tests": "75+ (dbt_expectations)",
            "RBAC Roles": "4 (Admin → Engineer → Analyst → Reader)",
            "Recovery (RPO)": "<1hr (Zero-Copy Clone)",
        }
        for k, v in metrics.items():
            st.markdown(f"**{k}:** {v}")

    st.markdown("---")

    # 3-Gate Validation
    st.subheader("3-Gate Data Validation Framework")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### Gate 1: Bronze (Schema)")
        st.markdown("- Required columns check\n- Data type validation\n- NOT NULL enforcement\n- Schema completeness")
        st.markdown("**FAIL →** `QUARANTINE_BRONZE`")
    with col2:
        st.markdown("### Gate 2: Silver (Business)")
        st.markdown("- Status filtering\n- Deduplication (ROW_NUMBER)\n- SCD Type 2 merge\n- Referential integrity\n- Aggregation guards")
        st.markdown("**FAIL →** `QUARANTINE_SILVER`")
    with col3:
        st.markdown("### Gate 3: Gold (Statistical)")
        st.markdown("- SPC Alert Engine\n- 6 alert categories\n- Composite severity\n- SLA Protection\n- Zero-Copy Clone fallback")
        st.markdown("**FAIL (CRITICAL) →** `BLOCK`")

    st.markdown("---")

    # SLA Protection
    st.subheader("SLA Protection Pattern")
    st.markdown("""
    **Key concept: `trigger_rule='all_done'`**

    The Gold Aggregation Task runs AFTER the Validation Task finishes — **even if it failed**.
    Combined with Snowflake Zero-Copy Clone caching, BI dashboards **always have data to serve**.

    - **Layer A:** Partial Async — `trigger_rule='all_done'` + quarantine routing
    - **Layer B:** Cached Gold — Zero-Copy Clone snapshot before each rebuild
    - **Layer C:** Dual-Platform — Databricks + Snowflake cross-failover
    """)

    st.markdown("---")
    st.subheader("Tech Stack")
    st.markdown("Snowflake · Databricks · dbt Core · Apache Airflow · PySpark · Delta Lake · Azure Data Factory · SSIS · Power BI · Streamlit · Docker · Terraform · Great Expectations · PagerDuty")

    st.markdown("---")
    st.markdown("**Built by Jay Pechnarai** · [GitHub](https://github.com/Kepler22j/petrova-pipeline) · [LinkedIn](https://linkedin.com/in/jay-pechnarai-4001986b)")
