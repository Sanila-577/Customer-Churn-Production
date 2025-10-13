<#
PowerShell equivalent of Makefile for Windows users.
Usage:
  ./make.ps1 install
  ./make.ps1 data-pipeline
  ./make.ps1 train-pipeline
  ./make.ps1 streaming-inference
  ./make.ps1 run-all
  ./make.ps1 clean
  ./make.ps1 mlflow-ui
  ./make.ps1 stop-all
#>

param(
    [string]$target = "help"
)

# Settings
$PYTHON = "python"
$VENV_DIR = ".venv"
$VENV_ACTIVATE = ".\.venv\Scripts\Activate.ps1"
$MLFLOW_PORT = 5001

function Help {
    Write-Host "Available targets:`n"
    Write-Host "  ./make.ps1 install             - Install dependencies & create venv"
    Write-Host "  ./make.ps1 data-pipeline       - Run the data pipeline"
    Write-Host "  ./make.ps1 train-pipeline      - Run the training pipeline"
    Write-Host "  ./make.ps1 streaming-inference - Run streaming inference pipeline"
    Write-Host "  ./make.ps1 run-all             - Run all pipelines in sequence"
    Write-Host "  ./make.ps1 mlflow-ui           - Launch MLflow UI"
    Write-Host "  ./make.ps1 stop-all            - Stop MLflow UI"
    Write-Host "  ./make.ps1 clean               - Clean up artifacts"
}

function Install {
    Write-Host "Installing project dependencies and setting up environment..." -ForegroundColor Cyan
    if (-Not (Test-Path $VENV_DIR)) {
        & $PYTHON -m venv $VENV_DIR
        Write-Host "Virtual environment created."
    } else {
        Write-Host "Virtual environment already exists."
    }

    # Dot-source the activate script so the venv is activated in the current session
    if (Test-Path $VENV_ACTIVATE) {
        . $VENV_ACTIVATE
    } else {
        Write-Host "Warning: Activate script not found at $VENV_ACTIVATE" -ForegroundColor Yellow
    }

    & $PYTHON -m pip install --upgrade pip
    & $PYTHON -m pip install -r requirements.txt
    Write-Host "`n✅ Installation completed successfully!"
    Write-Host "To activate manually: .\.venv\Scripts\Activate.ps1"
}

function Clean {
    Write-Host "Cleaning up artifacts..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force artifacts\data\*, artifacts\encode\*, artifacts\models\*, artifacts\mlflow_run_artifacts\*, artifacts\mlflow_training_artifacts\* -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force mlruns -ErrorAction SilentlyContinue
    Write-Host "✅ Cleanup completed!"
}

function DataPipeline {
    Write-Host "🚀 Running Data Pipeline..."
    if (Test-Path $VENV_ACTIVATE) { . $VENV_ACTIVATE }
    & $PYTHON -m pipelines.data_pipeline
    Write-Host "✅ Data pipeline completed successfully!"
}

function TrainPipeline{
    Write-Host "🚀 Running Training Pipeline..."
    if (Test-Path $VENV_ACTIVATE) { . $VENV_ACTIVATE }
    & $PYTHON -m pipelines.training_pipeline
    Write-Host "✅ Training pipeline completed successfully!"

}

function StreamingInference{
    Write-Host "🚀 Running Inference Pipeline..."
    if (Test-Path $VENV_ACTIVATE) { . $VENV_ACTIVATE }
    & $PYTHON -m pipelines.streaming_inference_pipeline
    Write-Host "✅ Inference pipeline completed successfully!"

}

function MlflowUI {
    Write-Host "Launching MLflow UI..." -ForegroundColor Green
    Write-Host "MLflow UI will be available at: http://localhost:$MLFLOW_PORT"
    Write-Host "Press Ctrl+C to stop the server"
    if (Test-Path $VENV_ACTIVATE) { . $VENV_ACTIVATE }
    # Use configured port
    & mlflow ui --backend-store-uri file:./mlruns --default-artifact-root ./artifacts --host 127.0.0.1 --port $MLFLOW_PORT
}
# 	@echo "Launching MLflow UI..."
# 	@echo "MLflow UI will be available at: http://localhost:$(MLFLOW_PORT)"
# 	@echo "Press Ctrl+C to stop the server"
# 	@source $(VENV) && mlflow ui --host 0.0.0.0 --port $(MLFLOW_PORT)

function StopAll {
    Write-Host "Stopping all MLflow servers..." -ForegroundColor Red
    # Try to find mlflow processes by process name or by path (path may be null for some processes)
    $mlflowProcesses = Get-Process -ErrorAction SilentlyContinue | Where-Object { ($_.ProcessName -like "*mlflow*") -or ($_.Path -and $_.Path -like "*mlflow*") }
    if ($mlflowProcesses -and $mlflowProcesses.Count -gt 0) {
        $mlflowProcesses | ForEach-Object { 
            Write-Host "Stopping process ID $($_.Id) - $($_.ProcessName)"
            try { Stop-Process -Id $_.Id -Force } catch { Write-Host "Failed to stop process $($_.Id): $_" }
        }
        Write-Host "✅ All MLflow servers have been stopped"
    } else {
        Write-Host "No running MLflow servers found."
    }
}

function RunAll {
    Write-Host "Running all pipelines: data -> train -> streaming" -ForegroundColor Cyan
    DataPipeline
    TrainPipeline
    StreamingInference
    Write-Host "✅ All pipelines completed"
}
# # Stop all running MLflow servers
# stop-all:
# 	@echo "Stopping all MLflow servers..."
# 	@echo "Finding MLflow processes on port $(MLFLOW_PORT)..."
# 	@-lsof -ti:$(MLFLOW_PORT) | xargs kill -9 2>/dev/null || true
# 	@echo "Finding other MLflow UI processes..."
# 	@-ps aux | grep '[m]lflow ui' | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
# 	@-ps aux | grep '[g]unicorn.*mlflow' | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
# 	@echo "✅ All MLflow servers have been stopped"

switch ($target) {
    "help" { Help }
    "install" { Install }
    "clean" { Clean }
    "data-pipeline" { DataPipeline }
    "train-pipeline" { TrainPipeline }
    "streaming-inference" { StreamingInference }
    "run-all" { RunAll }
    "mlflow-ui" { MlflowUI }
    "stop-all" { StopAll }
    default { Help }
}


# .PHONY: data-pipeline-rebuild
# data-pipeline-rebuild:
# 	@source $(VENV) && $(PYTHON) -c "from pipelines.data_pipeline import data_pipeline; data_pipeline(force_rebuild=True)"

# # Run training pipeline
# train-pipeline:
# 	@echo "Running training pipeline..."
# 	@source $(VENV) && $(PYTHON) pipelines/training_pipeline.py

# # Run streaming inference pipeline with sample JSON
# streaming-inference:
# 	@echo "Running streaming inference pipeline with sample JSON..."
# 	@source $(VENV) && $(PYTHON) pipelines/streaming_inference_pipeline.py

# # Run all pipelines in sequence
# run-all:
# 	@echo "Running all pipelines in sequence..."
# 	@echo "========================================"
# 	@echo "Step 1: Running data pipeline"
# 	@echo "========================================"
# 	@source $(VENV) && $(PYTHON) pipelines/data_pipeline.py
# 	@echo "\n========================================"
# 	@echo "Step 2: Running training pipeline"
# 	@echo "========================================"
# 	@source $(VENV) && $(PYTHON) pipelines/training_pipeline.py
# 	@echo "\n========================================"
# 	@echo "Step 3: Running streaming inference pipeline"
# 	@echo "========================================"
# 	@source $(VENV) && $(PYTHON) pipelines/streaming_inference_pipeline.py
# 	@echo "\n========================================"
# 	@echo "All pipelines completed successfully!"
# 	@echo "========================================"

# mlflow-ui:
# 	@echo "Launching MLflow UI..."
# 	@echo "MLflow UI will be available at: http://localhost:$(MLFLOW_PORT)"
# 	@echo "Press Ctrl+C to stop the server"
# 	@source $(VENV) && mlflow ui --host 0.0.0.0 --port $(MLFLOW_PORT)

# # Stop all running MLflow servers
# stop-all:
# 	@echo "Stopping all MLflow servers..."
# 	@echo "Finding MLflow processes on port $(MLFLOW_PORT)..."
# 	@-lsof -ti:$(MLFLOW_PORT) | xargs kill -9 2>/dev/null || true
# 	@echo "Finding other MLflow UI processes..."
# 	@-ps aux | grep '[m]lflow ui' | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
# 	@-ps aux | grep '[g]unicorn.*mlflow' | awk '{print $$2}' | xargs kill -9 2>/dev/null || true
# 	@echo "✅ All MLflow servers have been stopped"