
# Kafka Avro Order Processing




## Overview
This project implements a Kafka-based streaming system with:

- Avro serialization

- Producer that sends order events

- Consumer that computes running average

- Retry logic for transient errors

- Dead Letter Queue (DLQ) for permanently failed messages

- Kafka UI for live demonstration

- Schema Registry for Avro schema management.

- Fully implemented with Python.
## Architecture
### Topics:
- orders – Incoming events

- orders-avg – Running average updates

- orders-DLQ – Failed messages

### Services (Docker):
- Kafka

- ZooKeeper

- Schema Registry

- Kafka UI

## How to Run
Step 1 — Start Kafka
```bash
  docker compose up -d
```
Step 2 — Install dependencies
```bash
  python -m venv .venv
  .venv\Scripts\activate
  pip install -r requirements.txt
```
Step 3 — Run Producer
```bash
  python app/producer.py
```
Step 3 — Start Consumer
```bash
  python app/consumer_avg.py
```
## DLQ Demonstration
The producer intentionally sends invalid messages (negative price).

Consumer detects permanent error and sends them to orders-DLQ.

View using Kafka UI:

➡ http://localhost:8080

→ Topics → orders-DLQ
## Avro Schema
schemas/order.avsc
```bash
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

## Retry Logic
- Transient errors retried 5 times

- Exponential backoff

- After 5 failures → DLQ
## Demo
https://youtu.be/9jXc7WO8bsg




