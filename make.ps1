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
# Prefer an existing venv folder: check 'venv' then '.venv', default to 'venv'
if (Test-Path ".\venv") { $VENV_DIR = "venv" }
elseif (Test-Path ".\.venv") { $VENV_DIR = ".venv" }
else { $VENV_DIR = "venv" }
$VENV_ACTIVATE = ".\$VENV_DIR\Scripts\Activate.ps1"

# Detect whether a wrapper 'uv' is available on PATH. If so, prefer using
# `uv pip install ...` instead of calling pip directly through python.
if (Get-Command uv -ErrorAction SilentlyContinue) {
    $USE_UV = $true
    Write-Host "Detected 'uv' on PATH; installer commands will use 'uv'" -ForegroundColor Cyan
} else {
    $USE_UV = $false
}
$MLFLOW_PORT = 5001

function Help {
    Write-Host "Available targets: "
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
        Write-Host "Virtual environment created at '$VENV_DIR'."
    } else {
        Write-Host "Virtual environment '$VENV_DIR' already exists."
    }

    # Dot-source the activate script so the venv is activated in the current session
    if (Test-Path $VENV_ACTIVATE) {
        . $VENV_ACTIVATE
    } else {
        Write-Host "Warning: Activate script not found at $VENV_ACTIVATE" -ForegroundColor Yellow
    }

    # Prefer the venv's python executable when available so installs target the venv
    $venvPython = Join-Path (Get-Location) "$VENV_DIR\Scripts\python.exe"
    if (Test-Path $venvPython) {
        $PYTHON = $venvPython
        Write-Host "Using venv python: $PYTHON"
    } else {
        Write-Host "Using system python: $PYTHON" -ForegroundColor Yellow
    }

    # Use 'uv' wrapper if available, otherwise call pip via the selected python
    if ($USE_UV) {
        Write-Host "Installing packages using 'uv pip'..." -ForegroundColor Cyan
        & uv pip install --upgrade pip
        & uv pip install -r requirements.txt
    } else {
        & $PYTHON -m pip install --upgrade pip
        & $PYTHON -m pip install -r requirements.txt
    }
    Write-Host "`n✅ Installation completed successfully!"
    Write-Host "To activate manually: .\$VENV_DIR\Scripts\Activate.ps1"
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