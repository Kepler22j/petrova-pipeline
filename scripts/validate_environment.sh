#!/usr/bin/env bash
# ============================================================
# PETROVA – Environment Validation Script
# Checks that all required tools and configs are in place
# ============================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; ERRORS=$((ERRORS + 1)); }
warn() { echo -e "${YELLOW}!${NC} $1"; }

ERRORS=0

echo "═══════════════════════════════════════════"
echo "  PETROVA 300K – Environment Validation"
echo "═══════════════════════════════════════════"
echo ""

# Python
echo "── Python ──"
command -v python3 &>/dev/null && pass "Python3: $(python3 --version)" || fail "Python3 not found"
command -v pip &>/dev/null && pass "pip available" || fail "pip not found"

# dbt
echo "── dbt ──"
command -v dbt &>/dev/null && pass "dbt: $(dbt --version | head -1)" || fail "dbt not found"

# Airflow
echo "── Airflow ──"
command -v airflow &>/dev/null && pass "Airflow: $(airflow version 2>/dev/null)" || warn "Airflow not installed (OK if using Docker)"

# Terraform
echo "── Terraform ──"
command -v terraform &>/dev/null && pass "Terraform: $(terraform version -json | python3 -c 'import sys,json;print(json.load(sys.stdin)["terraform_version"])' 2>/dev/null)" || warn "Terraform not installed"

# Docker
echo "── Docker ──"
command -v docker &>/dev/null && pass "Docker: $(docker --version)" || warn "Docker not installed"

# Environment variables
echo "── Environment Variables ──"
[[ -n "${SNOWFLAKE_ACCOUNT:-}" ]] && pass "SNOWFLAKE_ACCOUNT set" || warn "SNOWFLAKE_ACCOUNT not set"
[[ -n "${SNOWFLAKE_USER:-}" ]] && pass "SNOWFLAKE_USER set" || warn "SNOWFLAKE_USER not set"
[[ -n "${DATABRICKS_HOST:-}" ]] && pass "DATABRICKS_HOST set" || warn "DATABRICKS_HOST not set"

# Config files
echo "── Config Files ──"
[[ -f "dbt/dbt_project.yml" ]] && pass "dbt_project.yml exists" || fail "dbt_project.yml missing"
[[ -f ".env.example" ]] && pass ".env.example exists" || fail ".env.example missing"
[[ -f ".gitignore" ]] && pass ".gitignore exists" || fail ".gitignore missing"

echo ""
if [[ $ERRORS -eq 0 ]]; then
    echo -e "${GREEN}All checks passed!${NC}"
else
    echo -e "${RED}${ERRORS} check(s) failed.${NC}"
    exit 1
fi
