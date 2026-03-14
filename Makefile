# ──────────────────────────────────────────────────────────────────────────
# Neith — Indian Stock Datalake Developer Makefile
# ──────────────────────────────────────────────────────────────────────────
DOCKER_COMPOSE = docker-compose -f docker/docker-compose.yml
PYTHON         = python
TODAY          = $(shell date +%Y-%m-%d)

.PHONY: help up down restart logs status \
        init-topics ingest ingest-date ingest-backfill \
        flink-equity flink-fo \
        spark-silver spark-gold compact \
        api api-dev \
        test test-unit test-lint \
        clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Docker Stack ───────────────────────────────────────────────────────────
up: ## Start all services (Kafka, Flink, Spark, MinIO, Airflow, Grafana)
	$(DOCKER_COMPOSE) up -d
	@echo ""
	@echo "✅  Neith stack started. Services:"
	@echo "   Kafka UI      → http://localhost:18080"
	@echo "   Schema Reg    → http://localhost:8081"
	@echo "   Iceberg REST  → http://localhost:8181"
	@echo "   Flink UI      → http://localhost:8082"
	@echo "   Spark UI      → http://localhost:8083"
	@echo "   Airflow       → http://localhost:8084  (admin/admin)"
	@echo "   MinIO Console → http://localhost:19001  (neith-access/neith-secret99)"
	@echo "   Prometheus    → http://localhost:19090"
	@echo "   Grafana       → http://localhost:3000  (admin/neith123)"

down: ## Stop all services
	$(DOCKER_COMPOSE) down

restart: ## Restart all services
	$(DOCKER_COMPOSE) restart

logs: ## Tail logs (optionally: make logs SERVICE=kafka)
	$(DOCKER_COMPOSE) logs -f $(SERVICE)

status: ## Show health of all containers
	$(DOCKER_COMPOSE) ps

# ── Ingestion ──────────────────────────────────────────────────────────────
init-topics: ## Create all Kafka topics with correct partitions
	$(PYTHON) scripts/init_topics.py

ingest: ## Run all domain producers for today
	$(PYTHON) -m producers.runner --date $(TODAY) --domains equity fo index corporate surveillance macro

ingest-date: ## Run producers for a specific date: make ingest-date DATE=2025-03-13
	$(PYTHON) -m producers.runner --date $(DATE)

ingest-backfill: ## Trigger Airflow historical backfill DAG
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger neith_historical_backfill

ingest-domain: ## Run single domain: make ingest-domain DOMAIN=equity DATE=2025-03-13
	$(PYTHON) -m producers.runner --date $(DATE) --domains $(DOMAIN)

# ── Flink Jobs ─────────────────────────────────────────────────────────────
flink-equity: ## Submit equity cleaner Flink job
	$(DOCKER_COMPOSE) exec flink-jobmanager \
		flink run -py /opt/flink/usrlib/equity_cleaner.py \
		--jarfile /opt/flink/usrlib/flink-sql-connector-kafka.jar

flink-fo: ## Submit FO cleaner Flink job
	$(DOCKER_COMPOSE) exec flink-jobmanager \
		flink run -py /opt/flink/usrlib/fo_cleaner.py \
		--jarfile /opt/flink/usrlib/flink-sql-connector-kafka.jar

# ── Spark Jobs ─────────────────────────────────────────────────────────────
spark-silver: ## Run OHLCV adjuster (bronze → silver)
	$(DOCKER_COMPOSE) exec spark-master \
		spark-submit --master spark://spark-master:7077 \
		--conf spark.executor.memory=2g \
		/opt/spark-apps/ohlcv_adjuster.py --start-date $(TODAY)

spark-gold: ## Run market breadth aggregation (silver → gold)
	$(DOCKER_COMPOSE) exec spark-master \
		spark-submit --master spark://spark-master:7077 \
		/opt/spark-apps/market_breadth.py

compact: ## Run Iceberg compaction and maintenance
	$(DOCKER_COMPOSE) exec spark-master \
		spark-submit --master spark://spark-master:7077 \
		/opt/spark-apps/compaction.py

# ── API ────────────────────────────────────────────────────────────────────
api: ## Start FastAPI production server (2 workers)
	uvicorn api.main:app --host 0.0.0.0 --port 18000 --workers 2

api-dev: ## Start FastAPI with hot reload (dev mode)
	uvicorn api.main:app --host 0.0.0.0 --port 18000 --reload

# ── Query shortcuts ────────────────────────────────────────────────────────
query-ohlcv: ## Quick query: curl localhost:18000/equities/RELIANCE/ohlcv?from_date=2024-01-01&to_date=2025-03-13
	curl -s "http://localhost:18000/equities/RELIANCE/ohlcv?from_date=2024-01-01&to_date=$(TODAY)" | python -m json.tool | head -60

query-surveillance: ## Show current ASM stocks
	curl -s "http://localhost:18000/surveillance?list_type=ASM_LT" | python -m json.tool | head -40

# ── Testing ────────────────────────────────────────────────────────────────
test: ## Run all tests
	pytest tests/ -v --tb=short

test-unit: ## Run unit tests only (no live NSE or Kafka needed)
	pytest tests/ -v --tb=short -m "not integration"

test-lint: ## Check code style with ruff
	ruff check producers/ flink_jobs/ spark_jobs/ airflow_dags/ api/ config/

# ── Cleanup ────────────────────────────────────────────────────────────────
clean: ## Remove all Docker volumes (WARNING: destroys all data)
	$(DOCKER_COMPOSE) down -v
	@echo "⚠️  All Neith data volumes removed."
