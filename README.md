# Real-Time Stock Market Insights & Reporting

A reliable, scalable ETL pipeline for processing and streaming stock market data with low latency.

## Project Overview

This project aims to implement a reliable and scalable ETL data pipeline for processing and streaming stock market data with low latency.

Real-time insights visualization on stock trends, trading volumes, and other financial metrics.

The implementation of this project is a Python-based ETL pipeline that extracts stock market data in JSON format from the Alpha Vantage API. Python is used mainly to filter the extracted data and select the required fields before publishing the records to Apache Kafka.

## Data Pipeline Architecture

![Data Pipeline Architecture](./img/Architecture.drawio.png)

The project components are containerized and managed using Docker Compose, allowing Kafka, Spark, PostgreSQL, and pgAdmin to run together in the same development environment.

## Tech Stack and Flow

- **Python** — data integration, processing, and API interaction
- **Apache Kafka** — inspect and stream real-time data from multiple sources
- **API (Alpha Vantage)** — produces JSON events into Kafka
- **Apache Spark** — consumes data from Kafka and loads into Postgres
- **PostgreSQL** — stores results for analytics and reporting
- **pgAdmin** — visual Postgres management
- **Power BI** — external BI layer (connects to Postgres)
