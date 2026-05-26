#!/usr/bin/env bash
# ============================================================
# PETROVA – Local End-to-End Pipeline Runner
# One command: ./run_local.sh
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔══════════════════════════════════════════════╗"
echo "║  PETROVA Pipeline – Local E2E Runner         ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── Step 1: Start PostgreSQL ──
echo "▶ Step 1/5: Starting PostgreSQL..."
cd airflow
docker compose up -d postgres
echo "  Waiting for PostgreSQL to be healthy..."
until docker compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1; do
    sleep 1
done
echo "  ✓ PostgreSQL ready"

# ── Step 2: Create petrova database + schemas ──
echo "▶ Step 2/5: Initializing petrova database..."
docker compose exec -T postgres psql -U airflow -d airflow -c \
    "SELECT 'CREATE DATABASE petrova' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'petrova')" \
    -t | xargs -I {} docker compose exec -T postgres psql -U airflow -d airflow -c "{}" 2>/dev/null || true

docker compose exec -T postgres psql -U airflow -d petrova << 'INITEOF'
DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'petrova') THEN CREATE ROLE petrova WITH LOGIN PASSWORD 'petrova'; END IF; END $$;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL PRIVILEGES ON DATABASE petrova TO petrova;
GRANT ALL PRIVILEGES ON SCHEMA public, bronze, silver, gold TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO petrova;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO petrova;
INITEOF
echo "  ✓ Database initialized (petrova + bronze/silver/gold schemas)"

# ── Step 3: Install dbt + deps ──
echo "▶ Step 3/5: Setting up dbt..."
cd "$SCRIPT_DIR/dbt"

if ! command -v dbt &> /dev/null; then
    echo "  Installing dbt-postgres..."
    pip install dbt-postgres --quiet
fi

# Set up local profile
mkdir -p ~/.dbt
cp profiles_local.yml ~/.dbt/profiles.yml
echo "  ✓ dbt profile configured (target: local)"

# Install dbt packages
dbt deps --target local
echo "  ✓ dbt packages installed"

# ── Step 4: Run dbt pipeline ──
echo "▶ Step 4/5: Running dbt pipeline..."
echo ""

echo "  ── dbt seed (loading 7 seed tables) ──"
dbt seed --target local
echo ""

echo "  ── dbt run (16 models: staging→intermediate→marts) ──"
dbt run --target local
echo ""

echo "  ── dbt test (51 data quality tests) ──"
dbt test --target local
echo ""

# ── Step 5: Summary ──
echo "▶ Step 5/5: Pipeline verification..."
docker compose exec -T postgres psql -U petrova -d petrova << 'VERIFYEOF'
\echo '─── Schema: public (seeds/sources) ───'
SELECT schemaname, count(*) as tables FROM pg_tables WHERE schemaname = 'public' GROUP BY 1;
\echo '─── Schema: bronze (staging views) ───'
SELECT schemaname, count(*) as views FROM pg_views WHERE schemaname = 'bronze' GROUP BY 1;
\echo '─── Schema: silver (intermediate tables) ───'
SELECT schemaname, count(*) as tables FROM pg_tables WHERE schemaname = 'silver' GROUP BY 1;
\echo '─── Schema: gold (marts tables) ───'
SELECT schemaname, count(*) as tables FROM pg_tables WHERE schemaname = 'gold' GROUP BY 1;
\echo '─── Gold: fct_daily_sensor_kpi sample ───'
SELECT kpi_date, sensor_id, total_readings, round(avg_reading::numeric, 2) as avg_reading FROM gold.fct_daily_sensor_kpi LIMIT 5;
\echo '─── Gold: fct_sensor_alerts sample ───'
SELECT kpi_date, sensor_id, alert_severity, stability_level, spike_status FROM gold.fct_sensor_alerts LIMIT 5;
VERIFYEOF

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  ✓ PETROVA Pipeline – ALL STEPS COMPLETE     ║"
echo "║                                              ║"
echo "║  Seeds:  7 loaded    Models: 16 built        ║"
echo "║  Tests:  51 run      Schemas: 4 populated    ║"
echo "╚══════════════════════════════════════════════╝"
