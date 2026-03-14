# Neith — Indian Stock Datalake

Neith is an event-driven, distributed datalake for Indian stock market data. It provides end-to-end ingestion, processing, and analytics using a modern data stack.

---

## 🏗 Architecture Overview

Neith leverages a robust stack for high-performance data processing:
- **Ingestion**: Python producers using `financeindia` and `confluent-kafka`.
- **Stream Processing**: **Apache Flink** for real-time data cleaning and normalization.
- **Batch Processing**: **Apache Spark** for OHLCV adjustments and market breadth aggregations.
- **Storage**: **Apache Iceberg** tables stored in **MinIO** (S3-compatible).
- **Orchestration**: **Apache Airflow** for historical backfills.
- **API**: **FastAPI** for low-latency data retrieval.
- **Observability**: **Prometheus**, **Grafana**, and **Kafka UI**.

---

## 🛠 Infrastructure Status

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Kafka UI** | [http://localhost:18080](http://localhost:18080) | - |
| **Flink UI** | [http://localhost:8082](http://localhost:8082) | - |
| **Spark UI** | [http://localhost:8083](http://localhost:8083) | - |
| **Airflow** | [http://localhost:8084](http://localhost:8084) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:19001](http://localhost:19001) | `neith-access` / `neith-secret99` |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `neith123` |
| **API Docs** | [http://localhost:18000/docs](http://localhost:18000/docs) | - |

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- **Docker & Docker Compose**
- **Python 3.11+**
- **uv** (Recommended for dependency management)

### 2. Setup
Clone the repository and install dependencies:
```bash
# Install dependencies
uv sync
```

Ensure your `.env` file is configured (copy from `.env.example` if available).

### 3. Spin Up Infrastructure
Start the Kafka cluster, Spark, Flink, and MinIO:
```bash
make up
```

### 4. Initialize Kafka Topics
Create the necessary topics before running producers:
```bash
make init-topics
```

### 5. Ingest Data
Run producers to fetch today's data from NSE:
```bash
make ingest
```
*To ingest a specific date:* `make ingest-date DATE=2024-03-14`

### 6. Run Processing Jobs
**Stream Processing (Flink):**
```bash
make flink-equity  # Process equity data
make flink-fo      # Process derivatives data
```

**Batch Processing (Spark):**
```bash
make spark-silver  # Generate adjusted OHLCV
make spark-gold    # Generate market breadth
```

### 7. Start the API
Launch the data access layer:
```bash
make api-dev
```
Test a query:
```bash
make query-ohlcv
```

---

## 🧪 Developer Workflows

- **Run Tests**: `make test`
- **Linting**: `make test-lint`
- **Cleanup**: `make clean` (⚠️ Warning: Deletes all data volumes)

---

## ❓ Troubleshooting

- **Container Failures**: Ensure ports 18000, 18080, 8081-8084, 19000-19001 are not in use.
- **Kafka Connectivity**: If producers can't connect, ensure `NEITH_KAFKA_BROKERS` in `.env` matches your Docker network setup.
- **Resource Issues**: The full stack requires ~8GB RAM. Adjust Spark/Flink memory in `Makefile` if necessary.
