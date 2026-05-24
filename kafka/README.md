# Kafka Configuration

Project Owner: Sanila Wijesekara

This directory contains the native Apache Kafka configuration used by the project. The broker is configured in KRaft mode, so ZooKeeper is not required.

Files in this directory:

- `server.properties` - KRaft broker and controller configuration

## Prerequisites

- Java 17 or higher
- Apache Kafka 3.7+ installed natively
- Python 3.8+ for the ML pipeline components

## Installation Options

### Option 1: macOS with Homebrew

```bash
brew install openjdk@17
brew install kafka

export KAFKA_HOME="$(brew --prefix kafka)/libexec"
export PATH="$KAFKA_HOME/bin:$PATH"

kafka-topics.sh --version
```

### Option 2: Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk

export KAFKA_VER=3.7.0
cd ~
curl -O https://downloads.apache.org/kafka/$KAFKA_VER/kafka_2.13-$KAFKA_VER.tgz
tar -xzf kafka_2.13-$KAFKA_VER.tgz
mv kafka_2.13-$KAFKA_VER kafka

export KAFKA_HOME="$HOME/kafka"
export PATH="$KAFKA_HOME/bin:$PATH"

$KAFKA_HOME/bin/kafka-topics.sh --version
```

### Option 3: Manual Download

1. Download Kafka from https://kafka.apache.org/downloads
2. Choose the Scala 2.13 binary package, for example `kafka_2.13-3.7.0.tgz`
3. Extract it to a location such as `/opt/kafka` or `~/kafka`
4. Set environment variables:

```bash
export KAFKA_HOME="/path/to/your/kafka"
export PATH="$KAFKA_HOME/bin:$PATH"
```

## Repo Setup

From the project root:

```bash
make install
make airflow-init
make kafka-format
make kafka-start-bg
make kafka-topics
```

The Kafka topics used by the project are:

- `telco.raw.customers`
- `telco.churn.predictions`
- `telco.deadletter`

## KRaft Broker Configuration

The broker is configured through `kafka/server.properties`.

Important settings:

- `process.roles=broker,controller`
- `controller.quorum.voters=1@localhost:9093`
- `listeners=PLAINTEXT://:9092,CONTROLLER://:9093`
- `advertised.listeners=PLAINTEXT://localhost:9092`
- `log.dirs=runtime/kafka-logs`
- `auto.create.topics.enable=false`
- `log.retention.hours=168`

## Quick Start

### 1. Format Storage

```bash
make kafka-format
```

Use this once before starting the broker for the first time, or after cleaning the Kafka data directory.

### 2. Start the Broker

```bash
make kafka-start-bg
```

This starts the native Kafka broker in the background and writes logs to `runtime/kafka.log`.

### 3. Create Topics

```bash
make kafka-topics
```

### 4. Run the Streaming Demo

Batch mode:

```bash
make kafka-producer-batch
make kafka-consumer
```

Streaming mode:

```bash
make kafka-consumer-continuous
make kafka-producer-stream
```

The current project behavior is:

- `kafka-producer-stream` queues `kafka_consumer_streaming_dag` after the stream producer finishes.
- `kafka-consumer-continuous` queues `kafka_consumer_streaming_dag` after each non-empty inference cycle.
- `kafka-consumer` remains the batch consumer entrypoint.

If you prefer the shorter alias, `make kafka-produce-stream` maps to `make kafka-producer-stream`.

## Verification

### Check Installation

```bash
java -version
kafka-topics.sh --version
echo $KAFKA_HOME
echo $PATH | grep kafka
```

### Test Broker Connectivity

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Test Producer/Consumer

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --partitions 1 --replication-factor 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

## Directory Structure

After setup, the project uses:

```text
churn-prediction-with-kafka-streaming/
├── kafka/
│   ├── server.properties
│   └── README.md
├── runtime/
│   └── kafka-logs/
└── .airflow/
```

## Troubleshooting

### Java Not Found

Install Java 17+ and verify with `java -version`.

### Kafka Commands Not Found

Make sure `KAFKA_HOME` is set and `PATH` includes `$KAFKA_HOME/bin`.

### Port Already in Use

Check and free ports 9092 and 9093:

```bash
lsof -i :9092
lsof -i :9093
```

### Storage Format Issues

```bash
rm -rf runtime/kafka-logs/
make kafka-format
```

### Broker Logs

- Kafka logs: `runtime/kafka.log`
- Broker data: `runtime/kafka-logs/`
- Airflow logs: `.airflow/logs/`

## Notes

- This project uses native Kafka, not Docker.
- KRaft mode removes the ZooKeeper dependency.
- The broker is designed for local development and testing.
- Topic auto-creation is disabled; create topics with `make kafka-topics`.

## Next Steps

1. Install Kafka natively
2. Set `KAFKA_HOME` and update `PATH`
3. Run `make kafka-format`
4. Run `make kafka-start-bg`
5. Create topics with `make kafka-topics`
6. Run the batch or streaming pipeline from the project root
