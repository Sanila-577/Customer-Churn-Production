# Telco Churn Prediction with Kafka and Airflow using PySpark Analytics

This project implements a telco churn prediction pipeline with local Apache Airflow orchestration, native Apache Kafka streaming, and PySpark-based model training and inference.

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/e818a20f-0510-4d16-81b7-87cedece470c" />


<img width="1510" height="412" alt="Screenshot 2026-05-24 at 16 08 24" src="https://github.com/user-attachments/assets/f5c2c815-9314-4cde-8e9a-80b3658379b8" />


## Prerequisites

- Python 3.9+
- Java 17+
- Apache Kafka 3.7+ installed natively
- uv package manager recommended

## Quick Start

### 1. Environment Setup

```bash
make install
```

This creates or uses `.venv` and installs the project dependencies.

### 2. Initialize Airflow

```bash
make airflow-init
make airflow-start
```

Airflow runs locally with the project-local metadata database in `.airflow/`. The UI is available at http://localhost:8080.

### 3. Setup Kafka

For Kafka installation and native KRaft setup steps, see [kafka/README.md](kafka/README.md).

```bash
source setup_kafka_env.sh
make kafka-format
make kafka-start-bg
make kafka-topics
```

Kafka topics used by this project are:

- `telco.raw.customers`
- `telco.churn.predictions`
- `telco.deadletter`

### 4. Run the Pipeline

#### Option A: Airflow UI

- Open http://localhost:8080
- Trigger the desired DAG from the UI

Current DAG ids:

- `kafka_batch_consumer_dag`
- `kafka_consumer_streaming_dag`

#### Option B: Command Line

Batch processing:

```bash
make kafka-producer-batch
make kafka-consumer
```

Streaming processing:

```bash
make kafka-consumer-continuous
make kafka-producer-stream
```

The streaming consumer now queues `kafka_consumer_streaming_dag` at the end of every non-empty inference cycle, so the Airflow UI gets a run as soon as a cycle finishes. The streaming producer also queues the same DAG after the stream run completes. If you prefer the alias used in the workflow, `make kafka-produce-stream` maps to `make kafka-producer-stream`.

## Key Commands

### ML Pipelines

```bash
make data-pipeline
make train-pipeline
make streaming-inference
```

### Kafka Operations

```bash
make kafka-start-bg
make kafka-stop
make kafka-topics
make kafka-producer-batch
make kafka-producer-stream
make kafka-produce-stream
make kafka-consumer
make kafka-consumer-continuous
make kafka-check
make kafka-reset
```

### Airflow Operations

```bash
make airflow-init
make airflow-start
make airflow-kill
make airflow-reset
make airflow-health
make airflow-trigger-all
make airflow-trigger-kafka-batch-consumer
make airflow-trigger-kafka-consumer-streaming
```

### Monitoring

```bash
make mlflow-ui
make kafka-sample-scored
```

## Data Flow

1. Data ingestion from `data/raw/TelcoCustomerChurn.csv`
2. Feature engineering and preprocessing in `src/`
3. Model training with PySpark via `make train-pipeline`
4. Kafka event production to `telco.raw.customers`
5. Continuous or batch inference through Kafka consumers
6. Predictions written to `telco.churn.predictions`

## Airflow DAGs

<img width="1510" height="412" alt="Screenshot 2026-05-24 at 16 08 24" src="https://github.com/user-attachments/assets/f5c2c815-9314-4cde-8e9a-80b3658379b8" />

The DAGs in `dags/` are copied into `.airflow/dags/` during Airflow initialization.

| DAG id | Schedule | Owner | Purpose |
| --- | --- | --- | --- |
| `kafka_batch_consumer_dag` | Hourly | sanila wijesekara | Batch Kafka scoring flow |
| `kafka_consumer_streaming_dag` | Every minute | sanila wijesekara | Streaming Kafka scoring flow |

The data, training, and inference pipelines are still available as direct `make` targets, but they are no longer tracked as Airflow DAGs in `dags/`.

Streaming behavior:

- `kafka-producer-stream` queues `kafka_consumer_streaming_dag` after the stream producer exits.
- `kafka-consumer-continuous` queues `kafka_consumer_streaming_dag` after every non-empty inference cycle.
- `kafka-consumer` remains the batch consumer entrypoint and maps to `kafka_batch_consumer_dag`.

## Configuration

Main configuration file: `config.yaml`

Important settings:

- Training engine: PySpark by default
- Model type: Random Forest
- Data split: 80/20
- Kafka bootstrap server: `localhost:9092`
- Airflow home: `.airflow/`

The project owner in the DAGs is `sanila wijesekara`.

## Logs and Monitoring

- Airflow web UI: http://localhost:8080
- Airflow logs: `.airflow/logs/<dag_id>/<task_id>/`
- Airflow scheduler logs: `.airflow/logs/scheduler/`
- Kafka logs: `runtime/kafka.log`
- MLflow UI: http://localhost:5001

## Troubleshooting

### Airflow DAG won't run

- Ensure `AIRFLOW_HOME="$PWD/.airflow"`
- Ensure `PYTHONPATH="$PWD"`
- Check scheduler status and DAG logs under `.airflow/logs/`

### Kafka broker won't start

- Ensure Java 17+ is installed
- Verify `KAFKA_HOME` is set when using the native Kafka scripts
- Check for port conflicts on `9092` and `9093`
- Review `runtime/kafka.log`

If `kafka_consumer_streaming_dag` does not appear in the UI, make sure the Kafka broker is running before using the streaming producer target. The stream trigger is queued after the producer completes and after each non-empty continuous consumer cycle.

### Model not found

- Run `make train-pipeline`
- Confirm `artifacts/models/` contains the trained model artifacts

## Dependencies

- Apache Airflow 2.10+
- PySpark 3.5+
- confluent-kafka
- pandas
- numpy
- scikit-learn
- MLflow

See `requirements.txt` for the full list.

## Author

**Sanila Wijesekara**
