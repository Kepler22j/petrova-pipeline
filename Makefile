# PETROVA — Developer Commands
# Usage: make <target>

.PHONY: help setup lint test dbt-run dbt-test docker-up docker-down clean

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ──────────────────────────────────────────────

setup: ## Install all Python dependencies
	pip install -r requirements.txt
	cd dbt && dbt deps

# ── Quality ────────────────────────────────────────────

lint: ## Run SQLFluff + Ruff linters
	sqlfluff lint dbt/models/ --dialect snowflake
	ruff check .

lint-fix: ## Auto-fix lint issues
	sqlfluff fix dbt/models/ --dialect snowflake
	ruff check --fix .

test: ## Run all tests (dbt + pytest)
	cd dbt && dbt test
	pytest tests/ -v

# ── dbt ────────────────────────────────────────────────

dbt-run: ## Run all dbt models
	cd dbt && dbt run

dbt-test: ## Run dbt tests only
	cd dbt && dbt test

dbt-docs: ## Generate and serve dbt docs
	cd dbt && dbt docs generate && dbt docs serve

dbt-seed: ## Load seed data (quality_thresholds, sensor_status_codes)
	cd dbt && dbt seed

dbt-snapshot: ## Run dbt snapshots (SCD2)
	cd dbt && dbt snapshot

# ── Docker (Airflow) ──────────────────────────────────

docker-up: ## Start Airflow + PySpark Docker environment
	cd airflow && docker compose up -d --build

docker-down: ## Stop Docker environment
	cd airflow && docker compose down

docker-logs: ## Tail Airflow logs
	cd airflow && docker compose logs -f webserver scheduler

# ── Maintenance ────────────────────────────────────────

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ipynb_checkpoints -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	cd dbt && dbt clean

validate: ## Run environment validation script
	bash scripts/validate_environment.sh
