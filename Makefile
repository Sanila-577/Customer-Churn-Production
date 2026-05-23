# Makefile for macOS / Linux (POSIX)
# Usage: `make <target>`

PY := $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null || echo python)
VENV_DIR := $(shell if [ -d ".venv" ]; then echo ".venv"; elif [ -d "venv" ]; then echo "venv"; else echo ".venv"; fi)
VENV_PY := $(VENV_DIR)/bin/python
UV := $(shell command -v uv 2>/dev/null || true)
AIRFLOW := $(VENV_PY) -m airflow
MLFLOW_PORT ?= 5001

.PHONY: help install clean data-pipeline train-pipeline streaming-inference run-all mlflow-ui stop-all

help:
	@echo "Available targets:"
	@echo "  make install             - create venv and install dependencies"
	@echo "  make data-pipeline       - run data pipeline"
	@echo "  make train-pipeline      - run training pipeline"
	@echo "  make streaming-inference - run streaming inference pipeline"
	@echo "  make run-all             - run data -> train -> streaming"
	@echo "  make mlflow-ui           - launch MLflow UI (localhost:$(MLFLOW_PORT))"
	@echo "  make stop-all            - stop MLflow processes"
	@echo "  make clean               - remove artifacts and mlruns"

install:
	@echo "Setting up virtual environment: $(VENV_DIR)"
	@test -d $(VENV_DIR) || $(PY) -m venv $(VENV_DIR)
	@echo "Using python: $(VENV_PY)"
	@$(VENV_PY) -m pip install --upgrade pip setuptools wheel
	@if [ -n "$(UV)" ]; then \
		echo "Detected 'uv' on PATH — installing via 'uv pip'"; \
		uv pip install -r requirements.txt; \
	else \
		$(VENV_PY) -m pip install -r requirements.txt; \
	fi
	@echo "✅ Installation completed. To activate: source $(VENV_DIR)/bin/activate"

clean:
	@echo "Cleaning artifacts and mlruns..."
	@rm -rf artifacts/data/* artifacts/encode/* artifacts/models/* artifacts/mlflow_run_artifacts/* artifacts/mlflow_training_artifacts/* mlruns || true
	@echo "✅ Clean completed"

data-pipeline:
	@echo "🚀 Running Data Pipeline..."
	@$(VENV_PY) -m pipelines.data_pipeline
	@echo "✅ Data pipeline completed"

train-pipeline:
	@echo "🚀 Running Training Pipeline..."
	@$(VENV_PY) -m pipelines.training_pipeline
	@echo "✅ Training pipeline completed"

streaming-inference:
	@echo "🚀 Running Streaming Inference..."
	@$(VENV_PY) -m pipelines.streaming_inference_pipeline
	@echo "✅ Inference pipeline completed"

run-all: data-pipeline train-pipeline streaming-inference

mlflow-ui:
	@echo "Launching MLflow UI at http://localhost:$(MLFLOW_PORT)"
	@$(VENV_PY) -m mlflow ui --backend-store-uri file:./mlruns --default-artifact-root ./artifacts --host 127.0.0.1 --port $(MLFLOW_PORT)

# Stop all running MLflow servers
stop-all:
	@echo "Stopping MLflow processes (if any)"
	@pkill -f mlflow || true
	@echo "✅ Stop attempted"

# Configuration
KAFKA_CONF := kafka/server.properties
KAFKA_LOG_DIR := runtime/kafka-logs
PID_DIR := runtime/pids

kafka-format:
	@echo "🔧 Formatting native Kafka storage (KRaft mode)..."
	@if [ -z "$$KAFKA_HOME" ]; then \
		echo "❌ KAFKA_HOME not set. Please install Kafka natively and set KAFKA_HOME"; \
		echo "💡 Installation guide: README_KAFKA.md"; \
		exit 1; \
	fi
	@echo "📁 Creating runtime directories..."
	@mkdir -p runtime/kafka-logs runtime/pids
	@echo "🔑 Generating cluster UUID..."
	@CLUSTER_ID=$$($${KAFKA_HOME}/bin/kafka-storage.sh random-uuid); \
	echo "Using Cluster ID: $$CLUSTER_ID"; \
	$${KAFKA_HOME}/bin/kafka-storage.sh format -t $$CLUSTER_ID -c "$(KAFKA_CONF)"
	@echo "✅ Native Kafka storage formatted successfully"

kafka-start-bg:
	@echo "🚀 Starting native Kafka broker in background..."
	@if [ -z "$$KAFKA_HOME" ]; then \
		echo "❌ KAFKA_HOME not set"; \
		exit 1; \
	fi
	@mkdir -p $(PID_DIR)
	@nohup $${KAFKA_HOME}/bin/kafka-server-start.sh "$(KAFKA_CONF)" > runtime/kafka.log 2>&1 & \
	echo $$! > $(PID_DIR)/kafka.pid
	@echo "✅ Kafka broker started in background (PID: $$(cat $(PID_DIR)/kafka.pid))"
	@echo "📄 Logs: runtime/kafka.log"

kafka-stop:
	@echo "🛑 Stopping native Kafka broker..."
	@if [ -z "$$KAFKA_HOME" ]; then \
		echo "❌ KAFKA_HOME not set"; \
		exit 1; \
	fi
	@if [ -f "$(PID_DIR)/kafka.pid" ]; then \
		PID=$$(cat $(PID_DIR)/kafka.pid); \
		echo "🔍 Found Kafka PID: $$PID"; \
		kill $$PID || true; \
		rm -f $(PID_DIR)/kafka.pid; \
		echo "✅ Kafka broker stopped"; \
	else \
		echo "⚠️ PID file not found, trying graceful shutdown..."; \
		$${KAFKA_HOME}/bin/kafka-server-stop.sh || true; \
	fi

kafka-topics:
	@echo "📋 Creating churn prediction topics on native broker..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker at localhost:9092"; \
		echo "💡 Please start broker with 'make kafka-start' in another terminal"; \
		exit 1; \
	fi
	@echo "🔮 Creating telco.raw.customers topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.raw.customers --partitions 1 --replication-factor 1 --if-not-exists
	@echo "🔮 Creating telco.churn.predictions topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.churn.predictions --partitions 1 --replication-factor 1 --if-not-exists
	@echo "🔮 Creating telco.deadletter topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.deadletter --partitions 1 --replication-factor 1 --if-not-exists
	@echo "✅ Churn predictions topics created successfully"
	@echo "📋 Current topics on native broker:"
	@kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-producer-stream:
	@echo "🌊 Starting Kafka streaming producer (real data sampling)..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker"; \
		echo "💡 Please start broker with 'make kafka-start'"; \
		exit 1; \
	fi
	@echo "🎯 Streaming real customer events to localhost:9092 (1 event/sec for 5 mins)"
	@$(VENV_PY) -m pipelines.producer --mode streaming --rate 1 --duration 300


kafka-producer-batch:
	@echo "📦 Starting Kafka batch producer (real data sampling)..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker"; \
		echo "💡 Please start broker with 'make kafka-start'"; \
		exit 1; \
	fi
	@echo "📊 Batch processing 100 real customer events to localhost:9092"
	@$(VENV_PY) -m pipelines.producer --mode batch --num-events 100

kafka-consumer:
	@echo "🌊 Starting Kafka batch consumer with ML predictions..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker"; \
		echo "💡 Please start broker with 'make kafka-start'"; \
		exit 1; \
	fi
	@echo "🎯 Processing messages in batches with ML predictions"
	@$(VENV_PY) -m pipelines.consumer

kafka-consumer-continuous:
	@echo "🔄 Starting continuous Kafka consumer monitoring..."
	@echo "📡 Monitoring for NEW messages (real-time ML processing)"
	@echo "🛑 Press Ctrl+C to stop monitoring"
	@$(VENV_PY) -m pipelines.consumer --continuous --poll-interval 5
	
kafka-check:
	@echo "🔍 Checking native Kafka broker status..."
	@if kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "✅ Native Kafka broker is running at localhost:9092"; \
		echo "📋 Available topics:"; \
		kafka-topics.sh --bootstrap-server localhost:9092 --list; \
		echo "📊 Broker information:"; \
		kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1; \
	else \
		echo "❌ Cannot connect to native Kafka broker at localhost:9092"; \
		echo "💡 Please start with 'make kafka-start' in another terminal"; \
		echo "💡 Or check installation with 'make kafka-validate'"; \
	fi
	
kafka-sample-scored:
	@echo "📊 Analyzing churn prediction results..."
	@if kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q telco.churn.predictions; then \
		$(VENV_PY) scripts/kafka_analytics.py; \
	else \
		echo "❌ telco.churn.predictions topic not found. Run 'make kafka-topics' first."; \
	fi

	kafka-cleanup-topics:
	@echo "🧹 Cleaning up unused Kafka topics..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker"; \
		echo "💡 Please start broker with 'make kafka-start'"; \
		exit 1; \
	fi
	@echo "Removing unused topics (keeping only telco.raw.customers / telco.churn.predictions)..."
	@for topic in customer_events model_updates data_quality_alerts; do \
		if kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q "$$topic"; then \
			echo "🗑️ Deleting topic: $$topic"; \
			kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$$topic"; \
		else \
			echo "✅ Topic $$topic not found (already clean)"; \
		fi; \
	done
	@echo "✅ Topic cleanup completed"
	@echo "📋 Remaining topics:"
	@kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-flush-messages:
	@echo "🗑️ Flushing all messages from Kafka topics..."
	@if ! kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then \
		echo "❌ Cannot connect to native Kafka broker"; \
		echo "💡 Please start broker with 'make kafka-start'"; \
		exit 1; \
	fi
	@echo "Deleting and recreating topics to flush all messages..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic telco.raw.customers 2>/dev/null || echo "Topic telco.raw.customers not found"
	@kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic telco.churn.predictions 2>/dev/null || echo "Topic telco.churn.predictions not found"
	@sleep 2
	@echo "🔮 Creating telco.raw.customers topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.raw.customers --partitions 1 --replication-factor 1
	@echo "🔮 Creating telco.churn.predictions topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.churn.predictions --partitions 1 --replication-factor 1
	@echo "🔮 Creating telco.deadletter topic..."
	@kafka-topics.sh --bootstrap-server localhost:9092 --create --topic telco.deadletter --partitions 1 --replication-factor 1
	@echo "✅ All messages flushed - topics are now empty"
	@echo "📋 Current topics:"
	@kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-reset:
	@echo "🧹 Resetting Kafka data (destructive operation)..."
	@read -p "⚠️ This will delete all Kafka data. Continue? (y/N): " confirm && [ "$$confirm" = "y" ]
	@echo "🛑 Stopping all Kafka processes..."
	@pkill -f kafka || echo "No Kafka processes found"
	@sleep 2
	@echo "🔧 Force killing port users..."
	@lsof -ti:9092,9093 | xargs kill -9 2>/dev/null || echo "Ports already free"
	@sleep 1
	@echo "🗑️ Removing Kafka data directory..."
	@rm -rf $(KAFKA_LOG_DIR)
	@echo "🗑️ Removing PID files..."
	@rm -f $(PID_DIR)/kafka.pid
	@echo "✅ Kafka reset completed. Run 'make kafka-format' to reinitialize"

kafka-help:
	@echo "🔧 Native Kafka Commands Help"
	@echo "=================================================="
	@echo "Installation Commands:"
	@echo "  kafka-install    - Install Kafka natively (first time)"
	@echo "  kafka-validate   - Validate installation"
	@echo ""
	@echo "Setup Commands:"
	@echo "  kafka-format     - Format Kafka storage (first time)"
	@echo "  kafka-start      - Start native Kafka broker"
	@echo "  kafka-start-bg   - Start broker in background"
	@echo "  kafka-stop       - Stop native Kafka broker"
	@echo "  kafka-topics     - Create churn prediction topic"
	@echo "  kafka-cleanup-topics - Remove unused topics"
	@echo ""
	@echo "Data Commands:"
	@echo "  kafka-producer-stream  - Start streaming producer (real data)"
	@echo "  kafka-producer-batch   - Start batch producer (real data)"
	@echo "  kafka-consumer         - Start batch ML consumer"
	@echo "  kafka-consumer-continuous - Start continuous ML consumer"
	@echo ""
	@echo "Monitoring Commands:"
	@echo "  kafka-check      - Check broker status"
	@echo "  kafka-monitor    - Monitor cluster health"
	@echo "  kafka-sample     - Sample input topic messages"
	@echo "  kafka-sample-scored - Show prediction analytics & statistics"
	@echo "  kafka-sample-raw - Sample raw scored messages"
	@echo "  kafka-test-event-driven - Test event-driven DAG logic"
	@echo ""
	@echo "Utility Commands:"
	@echo "  kafka-demo       - Show demo instructions"
	@echo "  kafka-reset      - Reset all Kafka data"
	@echo "  kafka-clean-restart - Complete cleanup and restart"
	@echo "  kafka-help       - Show this help"
	@echo ""
	@echo "📚 For detailed setup: README_KAFKA.md"

# ========================================================================================
# APACHE AIRFLOW ORCHESTRATION TARGETS
# ========================================================================================

airflow-init:
	@echo "🔧 Initializing Apache Airflow..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow"; \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH"; \
	. $(VENV_DIR)/bin/activate; \
	echo "Installing Apache Airflow..."; \
	$(VENV_PY) -m pip install "apache-airflow>=2.10.0,<3.0.0"; \
	$(VENV_PY) -m pip install apache-airflow-providers-apache-spark; \
	$(AIRFLOW) db init; \
	if ! $(AIRFLOW) users list | grep -q "admin"; then \
		$(AIRFLOW) users create -u admin -p admin -r Admin -e admin@example.com -f Admin -l User; \
	else \
		echo "User 'admin' already exists."; \
	fi; \
	mkdir -p .airflow/dags && find dags -name "*.py" -exec cp {} .airflow/dags/ \;; \
	echo "✅ Airflow initialized successfully!"

airflow-start:
	@echo "Checking for port conflicts..."
	@if lsof -ti:8080,8793,8794 >/dev/null 2>&1; then \
		echo "⚠️  Airflow ports are in use. Cleaning up first..."; \
		$(MAKE) airflow-kill; \
		sleep 3; \
	fi
	@echo "Ensuring DAGs are copied..."
	@find dags -name "*.py" -exec cp {} .airflow/dags/ \; 2>/dev/null || true
	@echo "Starting Airflow webserver + scheduler..."
	@echo "Webserver will be available at http://localhost:8080"
	@echo "Login with: admin / admin"
	@bash ./scripts/start_airflow.sh


airflow-kill:
	@echo "🛑 Killing all Airflow processes..."
	@pkill -f airflow || echo "No Airflow processes found"
	@sleep 2
	@echo "Force killing any remaining Airflow processes..."
	@pkill -9 -f airflow || echo "No remaining processes"
	@sleep 1
	@echo "Freeing Airflow ports (8080, 8793, 8794)..."
	@lsof -ti:8080,8793,8794 | xargs kill -9 2>/dev/null || echo "No processes using Airflow ports"
	@sleep 1
	@echo "Cleaning up PID files..."
	@rm -f .airflow/airflow-webserver.pid .airflow/airflow-scheduler.pid .airflow/airflow-triggerer.pid
	@echo "✅ All Airflow processes killed and ports freed successfully!"

airflow-reset:
	@echo "🔄 Resetting Airflow database and fixing login issues..."
	@$(MAKE) airflow-kill
	@echo "Removing old database and logs..."
	@rm -rf .airflow/airflow.db .airflow/logs/*
	@find . -path "./.venv" -prune -o -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -path "./.venv" -prune -o -name "*.pyc" -delete 2>/dev/null || true
	@echo "Reinitializing database..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	. $(VENV_DIR)/bin/activate && \
	$(AIRFLOW) db init
	@echo "Creating admin user..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	. $(VENV_DIR)/bin/activate && \
	$(AIRFLOW) users create -u admin -f Admin -l User -p admin -r Admin -e admin@example.com
	@echo "Copying DAGs..."
	@find dags -name "*.py" -exec cp {} .airflow/dags/ \;
	@echo "✅ Airflow reset complete! Login: admin/admin"
	@echo "Start with: make airflow-start"

airflow-webserver: ## Start Airflow webserver
	@echo "Starting Airflow webserver on http://localhost:8080..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	$(AIRFLOW) webserver --port 8080

airflow-scheduler: ## Start Airflow scheduler
	@echo "Starting Airflow scheduler..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	$(AIRFLOW) scheduler

	echo "⚠️ Bypassing Python version check..."; \


airflow-start-separate: ## Start Airflow webserver and scheduler separately
	@echo "Starting Airflow webserver and scheduler..."
	@echo "Webserver will be available at http://localhost:8080"
	@echo "Login with: admin / admin"
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export PATH="$(shell pwd)/.venv/bin:$$PATH" && \
	export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export AIRFLOW__WEBSERVER__WORKERS=1 && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	echo "Ensuring DB schema is upgraded before starting services..." && \
	$(AIRFLOW) db upgrade && \
	echo "Ensuring admin user exists (idempotent)..." && \
	$(AIRFLOW) users create -u admin -f Admin -l User -p admin -r Admin -e admin@example.com 2>/dev/null || true && \
	trap "kill 0" INT TERM EXIT && \
	echo "Starting webserver (debug mode on macOS to avoid Gunicorn fork issues)" && \
	$(AIRFLOW) webserver --port 8080 --debug & \
	webserver_pid=$$!; \
	until curl -fs http://localhost:8080/ >/dev/null 2>&1; do \
		if ! kill -0 $$webserver_pid >/dev/null 2>&1; then exit 1; fi; \
		sleep 1; \
	done && \
		echo "Starting scheduler after webserver and DB are ready" && \
	$(AIRFLOW) scheduler

airflow-dags-list: ## List all available DAGs
	@echo "Listing Airflow DAGs..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	$(AIRFLOW) dags list

airflow-test-data-pipeline: ## Test data pipeline DAG
	@echo "Testing data pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) tasks test data_pipeline_dag run_data_pipeline 2025-01-01

airflow-test-training-pipeline: ## Test training pipeline DAG
	@echo "Testing training pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) tasks test train_pipeline_dag run_training_pipeline 2025-01-01

airflow-test-inference-pipeline: ## Test inference pipeline DAG
	@echo "Testing inference pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) tasks test inference_pipeline_dag run_inference_pipeline 2025-01-01

airflow-clean: ## Clean Airflow database and logs
	@echo "Cleaning Airflow database and logs..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	rm -rf .airflow/airflow.db .airflow/logs/*

airflow-delete-dags: ## Delete all DAGs from Airflow UI (removes example DAGs too)
	@echo "Stopping Airflow if running..."
	@pkill -f airflow || true
	@echo "Configuring Airflow to hide example DAGs..."
	@. .venv/bin/activate && export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	if ! grep -q "load_examples = False" .airflow/airflow.cfg; then \
		sed -i '' 's/load_examples = True/load_examples = False/g' .airflow/airflow.cfg 2>/dev/null || \
		echo "load_examples = False" >> .airflow/airflow.cfg; \
	fi
	@echo "Deleting project DAG files..."
	@if [ -d ".airflow/dags" ]; then \
		rm -rf .airflow/dags/*; \
	fi
	@echo "All DAGs deleted. Example DAGs will be hidden on next start."
	@echo "To re-add your project DAGs, run: cp dags/* .airflow/dags/"
	@echo "To start Airflow without example DAGs, run: make airflow-standalone"

airflow-trigger-all: ## Trigger all DAGs manually for testing
	@echo "Triggering all DAGs..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	echo "Triggering data pipeline..." && \
	$(AIRFLOW) dags trigger data_pipeline_dag && \
	echo "Triggering training pipeline..." && \
	$(AIRFLOW) dags trigger train_pipeline_dag && \
	echo "Triggering inference pipeline..." && \
	$(AIRFLOW) dags trigger inference_pipeline_dag
	@echo "✅ All DAGs triggered! Check the Web UI at http://localhost:8080"

airflow-trigger-data-pipeline: ## Trigger data pipeline DAG manually
	@echo "Triggering data pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) dags trigger data_pipeline_dag
	@echo "✅ data_pipeline_dag triggered"

airflow-trigger-training-pipeline: ## Trigger training pipeline DAG manually
	@echo "Triggering training pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) dags trigger train_pipeline_dag
	@echo "✅ train_pipeline_dag triggered"

airflow-trigger-inference-pipeline: ## Trigger inference pipeline DAG manually
	@echo "Triggering inference pipeline DAG..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) dags trigger inference_pipeline_dag
	@echo "✅ inference_pipeline_dag triggered"

airflow-health: ## Check Airflow health status
	@echo "Checking Airflow health status..."
	@curl -s http://localhost:8080/health | $(VENV_PY) -m json.tool || echo "❌ Airflow not responding"
	@echo ""
	@echo "Checking running processes..."
	@ps aux | grep airflow | grep -v grep || echo "❌ No Airflow processes found"

re-run-all: ## 🔄 Complete reset: kill processes, clean everything, restart fresh
	@echo "🔄 Starting complete system reset and restart..."
	@echo "=================================================="
	@echo "Step 1/6: Killing all Airflow processes..."
	@$(MAKE) airflow-kill
	@echo ""
	@echo "Step 2/6: Cleaning database, logs, and Python cache files..."
	@rm -rf .airflow/airflow.db .airflow/logs/* .airflow/dags/* 2>/dev/null || true
	@find . -path "./.venv" -prune -o -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -path "./.venv" -prune -o -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Database, logs, and Python cache files cleaned"
	@echo ""
	@echo "Step 3/6: Reinitializing Airflow database..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) db migrate
	@echo "✅ Database reinitialized"
	@echo ""
	@echo "Step 4/6: Creating admin user..."
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(AIRFLOW) users create -u admin -f Admin -l User -p admin -r Admin -e admin@example.com 2>/dev/null || echo "Admin user already exists"
	@echo "✅ Admin user ready (admin/admin)"
	@echo ""
	@echo "Step 5/6: Copying fresh DAGs..."
	@find dags -name "*.py" -exec cp {} .airflow/dags/ \;
	@echo "✅ DAGs copied:"
	@ls -la .airflow/dags/*.py
	@echo ""
	@echo "Step 6/6: Starting Airflow with the stable webserver + scheduler flow..."
	@echo "🚀 Starting Airflow webserver and scheduler..."
	@echo "Webserver will be available at http://localhost:8080"
	@echo "Login with: admin / admin"
	@export AIRFLOW_HOME="$(shell pwd)/.airflow" && \
	export PATH="$(shell pwd)/.venv/bin:$$PATH" && \
	export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export AIRFLOW__WEBSERVER__WORKERS=1 && \
	export PYTHONPATH="$(shell pwd):$$PYTHONPATH" && \
	export PYTHONWARNINGS="ignore::DeprecationWarning" && \
	$(MAKE) airflow-start
	@echo ""
	@echo "=================================================="
	@echo "✅ COMPLETE RESET AND RESTART FINISHED!"
	@echo "🌐 Web UI: http://localhost:8080"
	@echo "🔑 Login: admin / admin"
	@echo "📊 Scheduling:"
	@echo "   - Data Pipeline: Every 5 minutes"
	@echo "   - Training Pipeline: Daily at 1 AM IST"
	@echo "   - Inference Pipeline: Every minute"
	@echo "=================================================="
	@echo "💡 Use 'make airflow-kill' to stop all processes"
	@echo "💡 Use 'make airflow-health' to check status"
