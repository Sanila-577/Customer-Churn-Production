# Makefile for macOS / Linux (POSIX)
# Usage: `make <target>`

PY := $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null || echo python)
VENV_DIR := $(shell if [ -d ".venv" ]; then echo ".venv"; elif [ -d "venv" ]; then echo "venv"; else echo ".venv"; fi)
VENV_PY := $(VENV_DIR)/bin/python
UV := $(shell command -v uv 2>/dev/null || true)
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

stop-all:
	@echo "Stopping MLflow processes (if any)"
	@pkill -f mlflow || true
	@echo "✅ Stop attempted"
