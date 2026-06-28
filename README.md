# Real-Time Stock Market Insights & Reporting


## Project Overview

An implementation of a real-time ETL pipeline that ingests intraday stock data from the Alpha Vantage API, streams it through Apache Kafka, processes it with Apache Spark, and loads it into PostgreSQL for analytics and reporting. Supporting Infrastructure and Spark consumer are containerised and managed using Docker Compose. The Python producer and Kafka verification scripts run manually on the local host machine.


### Project Objectives

- This project aims to implement a reliable and modular ETL data pipeline for processing and streaming stock market data 
with low latency.
- Real-time insights visualization on stock trends, trading volumes, and other financial metrics.


## Data Pipeline Architecture

![Data Pipeline and Architecture](img/Architecture.drawio.png)

The services are containerized and managed using Docker Compose, allowing Kafka, Kafka UI, Spark, PostgreSQL, pgAdmin and the Spark Consumer to run together in the same development environment.

### Tech Stack and Data Flow

Data moves through the pipeline in this order:


| Technology                          | Role in this project                                 |
| ----------------------------------- | ---------------------------------------------------- |
| Alpha Vantage (RapidAPI)            | Source of intraday stock JSON (TSLA, MSFT, GOOGL)    |
| Python + kafka-python               | Extract, transform, and publish records to Kafka     |
| Apache Kafka                        | Durable message stream on topic `stock_analysis`     |
| Apache Spark (Structured Streaming) | Consume Kafka, parse JSON, batch-write to PostgreSQL |
| PostgreSQL                          | Persist rows in the `stocks` table                   |
| pgAdmin / Power BI                  | Inspect data in the database / external BI reporting |


Supporting infrastructure (Docker Compose): Kafka UI, Spark master, Spark worker.

### Separation of Responsibilities


| Responsibility                 | Location                                                 | What it does                                                     |
| ------------------------------ | -------------------------------------------------------- | ---------------------------------------------------------------- |
| **API connection**             | `src/Producer/config.py`, `src/Producer/extract.py`      | Load RapidAPI credentials from `.env`; fetch intraday stock data |
| **Kafka producer**             | `src/Producer/main.py`, `src/Producer/producer_setup.py` | Publish JSON records to `stock_analysis` on `localhost:9094`     |
| **Kafka verification**         | `src/Consumer/kafka_consumer.py`                         | Local script to **confirm** messages arrive (print only)         |
| **Spark processing + DB load** | `src/Consumer/consumer.py`, `src/Consumer/dockerfile`    | Production job: Kafka → parse → JDBC append to `stocks`          |
| **Configuration and logging**  | `src/Producer/config.py`, `.env`                         | Secrets and API config; `LogFile.log` for producer logs          |
| **Infrastructure**             | `compose.yml`, `init/postgres/init.sql`                  | Kafka, Spark, Postgres, pgAdmin, consumer container              |


The **producer** and **kafka_consumer** scripts run on local host machine. The **Spark consumer** (`consumer.py`) runs automatically inside Docker via the `consumer` Compose service.

## Project Structure


| Directory                      | Functionality                                            |
| ------------------------------ | -------------------------------------------------------- | 
| `src/Producer/`                | API extract + Kafka publish (run manually)               | 
| `src/Consumer/`                | kafka_consumer.py (verify) + consumer.py (Spark job)     |
| `init/postgres/`               | stock table DDL                                          | 
| `compose.yml`                  | Docker stack                                             | 
| `.env`                         | secrets (not committed)                                  |
| `env.example`                  | template for required environment variables              | 


### Prerequisites

| Requirement               | Notes                                       |
|---------------------------|---------------------------------------------|
| Docker Desktop            | Must be running                             |
| Python 3.10+              | For producer and verification scripts       |
| RapidAPI key              | Alpha Vantage API subscription              |
| Git                       | To clone the repository                     |




## Setup (One-Time)

1. Clone the repo and open a terminal in the project root.
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
```

   Windows PowerShell:

1. Install Python dependencies:
  ```bash
   pip install -r requirements.txt
  ```
2. Copy `.env.example` to `.env` and fill in all variables (see [Configuration and Security](#configuration-and-security)).
3. Build the Spark consumer image:
  ```bash
   docker compose build consumer
  ```

## To run this project

The pipeline is **not fully automatic**. Follow these steps in order:

### 1. Start infrastructure

```bash
docker compose up -d
```

On first setup, use a clean volume so Postgres runs `init.sql`:

```bash
docker compose down -v
docker compose up -d
```

### 2. Wait for services to be ready

Allow 1–2 minutes for Kafka, Postgres, and the Spark consumer to start. The first consumer run downloads Spark JAR packages and takes longer.

### 3. Verify containers

```bash
docker compose ps
```

All services should show status **Up** (kafka, kafka-ui, spark-master, spark-worker, consumer, postgres_db, pgadmin).

### 4. Open Kafka UI

Go to [http://localhost:8085](http://localhost:8085)

- The cluster named **local** is pre-configured in `compose.yml`.
- If cluster validation fails, run `docker compose logs kafka` and restart the stack.

### 5. Optional — verify Kafka stream (Terminal 1)

```bash
python src/Consumer/kafka_consumer.py
```

Leave this running. You should see `Received: {...}` when data is published.

### 6. Publish data (Terminal 2)

```bash
python src/Producer/main.py
```

