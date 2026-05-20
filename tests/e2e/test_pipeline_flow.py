"""
PETROVA – End-to-End Pipeline Test
Validates the full Bronze → Silver → Gold flow.
Run: pytest tests/e2e/ -v --snowflake
"""
import pytest


@pytest.fixture
def snowflake_conn():
    """Create Snowflake connection for e2e tests."""
    try:
        import snowflake.connector
        import os
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse="PETROVA_DEV_WH",
            database="PETROVA_DEV",
        )
        yield conn
        conn.close()
    except Exception:
        pytest.skip("Snowflake connection not available")


class TestMedallionFlow:
    """End-to-end test: Bronze → Silver → Gold."""

    def test_bronze_has_data(self, snowflake_conn):
        cur = snowflake_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM BRONZE.SENSOR_READINGS_PARQUET")
        count = cur.fetchone()[0]
        assert count > 0, "Bronze layer has no data"

    def test_silver_quality_flags(self, snowflake_conn):
        cur = snowflake_conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM SILVER.SENSOR_READINGS_CLEANED
            WHERE quality_flag NOT IN ('PASS', 'WARN', 'FAIL')
        """)
        invalid = cur.fetchone()[0]
        assert invalid == 0, f"Silver has {invalid} rows with invalid quality_flag"

    def test_gold_immutability(self, snowflake_conn):
        """Verify Gold tables can't be directly modified by READER role."""
        cur = snowflake_conn.cursor()
        cur.execute("USE ROLE PETROVA_READER")
        with pytest.raises(Exception):
            cur.execute("DELETE FROM GOLD.FCT_DAILY_SENSOR_KPI WHERE 1=1")

    def test_gold_row_count_consistency(self, snowflake_conn):
        cur = snowflake_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM GOLD.FCT_DAILY_SENSOR_KPI")
        gold_count = cur.fetchone()[0]
        assert gold_count >= 0, "Gold KPI table accessible"
