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
