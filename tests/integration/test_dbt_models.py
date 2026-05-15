"""
PETROVA 300K – Integration Tests for dbt Models
Run: pytest tests/integration/ -v
"""
import subprocess
import pytest


class TestDbtCompile:
    """Verify all dbt models compile without errors."""

    def test_dbt_deps(self):
        result = subprocess.run(
            ["dbt", "deps", "--profiles-dir", ".", "--project-dir", "dbt"],
            capture_output=True, text=True, timeout=120
        )
        assert result.returncode == 0, f"dbt deps failed: {result.stderr}"

    def test_dbt_compile_bronze(self):
        result = subprocess.run(
            ["dbt", "compile", "--select", "tag:bronze",
             "--profiles-dir", ".", "--project-dir", "dbt"],
            capture_output=True, text=True, timeout=120
        )
        assert result.returncode == 0, f"Bronze compile failed: {result.stderr}"

    def test_dbt_compile_silver(self):
        result = subprocess.run(
            ["dbt", "compile", "--select", "tag:silver",
             "--profiles-dir", ".", "--project-dir", "dbt"],
            capture_output=True, text=True, timeout=120
        )
        assert result.returncode == 0, f"Silver compile failed: {result.stderr}"

    def test_dbt_compile_gold(self):
        result = subprocess.run(
            ["dbt", "compile", "--select", "tag:gold",
             "--profiles-dir", ".", "--project-dir", "dbt"],
            capture_output=True, text=True, timeout=120
        )
        assert result.returncode == 0, f"Gold compile failed: {result.stderr}"


class TestDbtTests:
    """Verify dbt tests pass (requires Snowflake connection)."""

    @pytest.mark.skipif(
        not subprocess.run(["dbt", "debug", "--profiles-dir", ".", "--project-dir", "dbt"],
                          capture_output=True).returncode == 0,
        reason="No Snowflake connection available"
    )
    def test_dbt_test_all(self):
        result = subprocess.run(
            ["dbt", "test", "--profiles-dir", ".", "--project-dir", "dbt"],
            capture_output=True, text=True, timeout=300
        )
        assert result.returncode == 0, f"dbt tests failed: {result.stderr}"
