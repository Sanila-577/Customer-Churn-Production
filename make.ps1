<#
PowerShell equivalent of the Makefile for Windows users.
#>

param(
    [string]$target = "help"
)

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$PYTHON = "python"
if (Test-Path ".\venv") { $VENV_DIR = "venv" }
elseif (Test-Path ".\.venv") { $VENV_DIR = ".venv" }
else { $VENV_DIR = "venv" }

$MLFLOW_PORT = 5001
$KAFKA_CONF = Join-Path $ProjectRoot "kafka\server.properties"
$KAFKA_LOG_DIR = Join-Path $ProjectRoot "runtime\kafka-logs"
$PID_DIR = Join-Path $ProjectRoot "runtime\pids"
$AIRFLOW_HOME = Join-Path $ProjectRoot ".airflow"

$USE_UV = [bool](Get-Command uv -ErrorAction SilentlyContinue)
if ($USE_UV) {
    Write-Host "Detected 'uv' on PATH; installer commands will use 'uv'"
}

function Get-PythonCommand {
    $venvPython = Join-Path $ProjectRoot "$VENV_DIR\Scripts\python.exe"
    if (Test-Path $venvPython) { return $venvPython }
    return $PYTHON
}

function Set-ProjectEnvironment {
    param([switch]$IncludeWebserverWorkers)
    $env:AIRFLOW_HOME = $AIRFLOW_HOME
    $env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
    $env:PYTHONPATH = if ([string]::IsNullOrWhiteSpace($env:PYTHONPATH)) { $ProjectRoot } else { "$ProjectRoot;$env:PYTHONPATH" }
    $env:PYTHONUNBUFFERED = "1"
    $env:PYTHONWARNINGS = "ignore::DeprecationWarning"
    if ($IncludeWebserverWorkers) { $env:AIRFLOW__WEBSERVER__WORKERS = "1" }
}

function Get-KafkaToolPath {
    param([Parameter(Mandatory = $true)][string]$ToolName)
    if (-not $env:KAFKA_HOME) { throw "KAFKA_HOME is not set. Set it before using Kafka targets." }
    $candidates = @(
        (Join-Path $env:KAFKA_HOME "bin\windows\$ToolName.bat"),
        (Join-Path $env:KAFKA_HOME "bin\windows\$ToolName.cmd"),
        (Join-Path $env:KAFKA_HOME "bin\$ToolName.bat"),
        (Join-Path $env:KAFKA_HOME "bin\$ToolName.cmd"),
        (Join-Path $env:KAFKA_HOME "bin\$ToolName")
    )
    foreach ($candidate in $candidates) { if (Test-Path $candidate) { return $candidate } }
    throw "Could not find $ToolName under KAFKA_HOME."
}

function Invoke-PythonModule {
    param(
        [Parameter(Mandatory = $true)][string]$ModuleName,
        [string[]]$Arguments = @()
    )
    $pythonCommand = Get-PythonCommand
    & $pythonCommand -m $ModuleName @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $pythonCommand -m $ModuleName" }
}

function Invoke-KafkaTool {
    param(
        [Parameter(Mandatory = $true)][string]$ToolName,
        [string[]]$Arguments = @()
    )
    $toolPath = Get-KafkaToolPath -ToolName $ToolName
    & $toolPath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed: $ToolName $($Arguments -join ' ')" }
}

function Ensure-AirflowDirectories {
    New-Item -ItemType Directory -Force -Path $AIRFLOW_HOME | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $AIRFLOW_HOME "dags") | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $AIRFLOW_HOME "logs") | Out-Null
}

function Copy-ProjectDags {
    Ensure-AirflowDirectories
    $sourceDags = Join-Path $ProjectRoot "dags"
    if (Test-Path $sourceDags) {
        Get-ChildItem -Path $sourceDags -Filter *.py -File -ErrorAction SilentlyContinue | ForEach-Object {
            Copy-Item $_.FullName -Destination (Join-Path $AIRFLOW_HOME "dags") -Force
        }
    }
}

function Start-AirflowWebserverBackground {
    Set-ProjectEnvironment -IncludeWebserverWorkers
    Ensure-AirflowDirectories
    $pythonCommand = Get-PythonCommand
    $stdoutLog = Join-Path $AIRFLOW_HOME "logs\airflow-webserver.out.log"
    $stderrLog = Join-Path $AIRFLOW_HOME "logs\airflow-webserver.err.log"
    return Start-Process -FilePath $pythonCommand -ArgumentList @("-m", "airflow", "webserver", "--port", "8080", "--debug") -WorkingDirectory $ProjectRoot -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -PassThru
}

function Start-AirflowSchedulerBackground {
    Set-ProjectEnvironment
    Ensure-AirflowDirectories
    $pythonCommand = Get-PythonCommand
    $stdoutLog = Join-Path $AIRFLOW_HOME "logs\airflow-scheduler.out.log"
    $stderrLog = Join-Path $AIRFLOW_HOME "logs\airflow-scheduler.err.log"
    return Start-Process -FilePath $pythonCommand -ArgumentList @("-m", "airflow", "scheduler") -WorkingDirectory $ProjectRoot -WindowStyle Hidden -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -PassThru
}

function Wait-ForAirflowWebserver {
    param([Parameter(Mandatory = $true)][int]$ProcessId)
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get -TimeoutSec 5 | Out-Null
            return
        } catch {
            if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { throw "Webserver died before becoming healthy" }
            Start-Sleep -Seconds 1
        }
    }
    throw "Timed out waiting for the Airflow webserver to become healthy"
}

function Stop-ProcessesByMatch {
    param([Parameter(Mandatory = $true)][string]$Pattern)
    $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -and $_.CommandLine -match $Pattern }
    foreach ($process in $processes) {
        try { Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue } catch { }
    }
}

function Stop-ProcessesOnPorts {
    param([Parameter(Mandatory = $true)][int[]]$Ports)
    try {
        Get-NetTCPConnection -LocalPort $Ports -State Listen -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique |
            ForEach-Object { try { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue } catch { } }
    } catch {
    }
}

function Remove-PathSafe {
    param([Parameter(Mandatory = $true)][string]$Path)
    Remove-Item -Path $Path -Recurse -Force -ErrorAction SilentlyContinue
}

function Help {
    Write-Host "Available targets:"
    Write-Host "  ./make.ps1 install"
    Write-Host "  ./make.ps1 clean"
    Write-Host "  ./make.ps1 data-pipeline"
    Write-Host "  ./make.ps1 train-pipeline"
    Write-Host "  ./make.ps1 streaming-inference"
    Write-Host "  ./make.ps1 run-all"
    Write-Host "  ./make.ps1 mlflow-ui"
    Write-Host "  ./make.ps1 stop-all"
    Write-Host "  ./make.ps1 kafka-help"
    Write-Host "  ./make.ps1 airflow-health"
    Write-Host ""
    Write-Host "Kafka targets: kafka-format, kafka-start-bg, kafka-stop, kafka-topics, kafka-producer-stream, kafka-produce-stream, kafka-producer-batch, kafka-consumer, kafka-consumer-continuous, kafka-consumer-stream, kafka-check, kafka-sample-scored, kafka-flush-messages, kafka-reset"
    Write-Host "Airflow targets: airflow-init, airflow-start, airflow-kill, airflow-reset, airflow-webserver, airflow-scheduler, airflow-start-separate, airflow-dags-list, airflow-test-data-pipeline, airflow-test-training-pipeline, airflow-test-inference-pipeline, airflow-clean, airflow-delete-dags, airflow-trigger-all, airflow-trigger-data-pipeline, airflow-trigger-training-pipeline, airflow-trigger-inference-pipeline, airflow-trigger-kafka-batch-consumer, airflow-trigger-kafka-consumer-streaming, re-run-all"
}

function Install {
    Write-Host "Installing project dependencies and setting up environment..."
    if (-not (Test-Path $VENV_DIR)) {
        & $PYTHON -m venv $VENV_DIR
        if ($LASTEXITCODE -ne 0) { throw "Failed to create virtual environment at $VENV_DIR" }
        Write-Host "Virtual environment created at '$VENV_DIR'."
    } else {
        Write-Host "Virtual environment '$VENV_DIR' already exists."
    }

    $pythonCommand = Get-PythonCommand
    Write-Host "Using python: $pythonCommand"
    if ($USE_UV) {
        Write-Host "Installing packages using 'uv pip'..."
        & uv pip install --upgrade pip setuptools wheel
        if ($LASTEXITCODE -ne 0) { throw "uv pip upgrade failed" }
        & uv pip install -r requirements.txt
        if ($LASTEXITCODE -ne 0) { throw "uv pip install failed" }
    } else {
        & $pythonCommand -m pip install --upgrade pip setuptools wheel
        if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
        & $pythonCommand -m pip install -r requirements.txt
        if ($LASTEXITCODE -ne 0) { throw "pip install failed" }
    }
    Write-Host "Installation completed successfully."
    Write-Host "To activate manually: .\$VENV_DIR\Scripts\Activate.ps1"
}

function Clean {
    Write-Host "Cleaning up artifacts..."
    Remove-PathSafe -Path (Join-Path $ProjectRoot "artifacts\data\*")
    Remove-PathSafe -Path (Join-Path $ProjectRoot "artifacts\encode\*")
    Remove-PathSafe -Path (Join-Path $ProjectRoot "artifacts\models\*")
    Remove-PathSafe -Path (Join-Path $ProjectRoot "artifacts\mlflow_run_artifacts\*")
    Remove-PathSafe -Path (Join-Path $ProjectRoot "artifacts\mlflow_training_artifacts\*")
    Remove-PathSafe -Path (Join-Path $ProjectRoot "mlruns")
    Write-Host "Cleanup completed."
}

function DataPipeline {
    Write-Host "Running Data Pipeline..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "pipelines.data_pipeline"
    Write-Host "Data pipeline completed successfully."
}

function TrainPipeline {
    Write-Host "Running Training Pipeline..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "pipelines.training_pipeline"
    Write-Host "Training pipeline completed successfully."
}

function StreamingInference {
    Write-Host "Running Inference Pipeline..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "pipelines.streaming_inference_pipeline"
    Write-Host "Inference pipeline completed successfully."
}

function RunAll {
    Write-Host "Running all pipelines: data -> train -> streaming"
    DataPipeline
    TrainPipeline
    StreamingInference
    Write-Host "All pipelines completed."
}

function MlflowUI {
    Write-Host "Launching MLflow UI..."
    Write-Host "MLflow UI will be available at: http://localhost:$MLFLOW_PORT"
    Write-Host "Press Ctrl+C to stop the server"
    Set-ProjectEnvironment
    & (Get-PythonCommand) -m mlflow ui --backend-store-uri file:./mlruns --default-artifact-root ./artifacts --host 127.0.0.1 --port $MLFLOW_PORT
}

function StopAll {
    Write-Host "Stopping all MLflow servers..."
    Stop-ProcessesByMatch -Pattern "mlflow"
    Write-Host "MLflow stop attempted."
}

function Test-KafkaBroker {
    try {
        Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--list")
        return $true
    } catch {
        return $false
    }
}

function KafkaFormat {
    Write-Host "Formatting native Kafka storage (KRaft mode)..."
    if (-not $env:KAFKA_HOME) { throw "KAFKA_HOME is not set." }
    New-Item -ItemType Directory -Force -Path $KAFKA_LOG_DIR | Out-Null
    New-Item -ItemType Directory -Force -Path $PID_DIR | Out-Null
    $storageTool = Get-KafkaToolPath -ToolName "kafka-storage"
    $clusterId = (& $storageTool random-uuid).Trim()
    if ([string]::IsNullOrWhiteSpace($clusterId)) { throw "Failed to generate a Kafka cluster ID" }
    Write-Host "Using Cluster ID: $clusterId"
    & $storageTool format -t $clusterId -c $KAFKA_CONF
    if ($LASTEXITCODE -ne 0) { throw "Kafka storage format failed" }
    Write-Host "Native Kafka storage formatted successfully."
}

function KafkaStartBg {
    Write-Host "Starting native Kafka broker in background..."
    if (-not $env:KAFKA_HOME) { throw "KAFKA_HOME is not set." }
    New-Item -ItemType Directory -Force -Path $PID_DIR | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "runtime") | Out-Null
    $serverTool = Get-KafkaToolPath -ToolName "kafka-server-start"
    $stdoutLog = Join-Path $ProjectRoot "runtime\kafka.out.log"
    $stderrLog = Join-Path $ProjectRoot "runtime\kafka.err.log"
    $process = Start-Process -FilePath $serverTool -ArgumentList @($KAFKA_CONF) -WorkingDirectory $ProjectRoot -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -WindowStyle Hidden -PassThru
    Set-Content -Path (Join-Path $PID_DIR "kafka.pid") -Value $process.Id
    Write-Host "Kafka broker started in background (PID: $($process.Id))"
    Write-Host "Logs: runtime\kafka.out.log and runtime\kafka.err.log"
}

function KafkaStop {
    Write-Host "Stopping native Kafka broker..."
    $pidFile = Join-Path $PID_DIR "kafka.pid"
    if (Test-Path $pidFile) {
        $pid = (Get-Content $pidFile | Select-Object -First 1).Trim()
        if ($pid) {
            try { Stop-Process -Id ([int]$pid) -Force -ErrorAction SilentlyContinue } catch { }
        }
        Remove-Item -Path $pidFile -Force -ErrorAction SilentlyContinue
        Write-Host "Kafka broker stopped"
    } elseif ($env:KAFKA_HOME) {
        & (Get-KafkaToolPath -ToolName "kafka-server-stop")
    }
    Stop-ProcessesOnPorts -Ports @(9092, 9093)
}

function KafkaTopics {
    Write-Host "Creating churn prediction topics on native broker..."
    if (-not (Test-KafkaBroker)) {
        Write-Host "Cannot connect to native Kafka broker at localhost:9092"
        Write-Host "Please start the broker with kafka-start-bg first"
        return
    }
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.raw.customers", "--partitions", "1", "--replication-factor", "1", "--if-not-exists")
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.churn.predictions", "--partitions", "1", "--replication-factor", "1", "--if-not-exists")
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.deadletter", "--partitions", "1", "--replication-factor", "1", "--if-not-exists")
    Write-Host "Current topics on native broker:"
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--list")
}

function KafkaProducerStream {
    $status = 0
    Write-Host "Starting Kafka streaming producer (real data sampling)..."
    if (-not (Test-KafkaBroker)) {
        Write-Host "Cannot connect to native Kafka broker"
        Write-Host "Please start the broker with kafka-start-bg first"
        $status = 1
    } else {
        Write-Host "Streaming real customer events to localhost:9092 (1 event/sec for 5 mins)"
        try { Invoke-PythonModule -ModuleName "pipelines.producer" -Arguments @("--mode", "streaming", "--rate", "1", "--duration", "300") } catch { $status = 1 }
    }
    Write-Host "Triggering kafka_consumer_streaming_dag in Airflow UI..."
    try { Set-ProjectEnvironment; Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "trigger", "kafka_consumer_streaming_dag") } catch { $status = 1 }
    Write-Host "kafka_consumer_streaming_dag trigger step finished"
    if ($status -ne 0) { throw "kafka-producer-stream completed with errors" }
}

function KafkaProduceStream { KafkaProducerStream }

function KafkaProducerBatch {
    Write-Host "Starting Kafka batch producer (real data sampling)..."
    if (-not (Test-KafkaBroker)) {
        Write-Host "Cannot connect to native Kafka broker"
        Write-Host "Please start the broker with kafka-start-bg first"
        return
    }
    Write-Host "Batch processing 100 real customer events to localhost:9092"
    Invoke-PythonModule -ModuleName "pipelines.producer" -Arguments @("--mode", "batch", "--num-events", "100")
}

function KafkaConsumer {
    Write-Host "Starting Kafka batch consumer with ML predictions..."
    if (-not (Test-KafkaBroker)) {
        Write-Host "Cannot connect to native Kafka broker"
        Write-Host "Please start the broker with kafka-start-bg first"
        return
    }
    Write-Host "Processing messages in batches with ML predictions"
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "pipelines.consumer"
}

function KafkaConsumerContinuous {
    Write-Host "Starting continuous Kafka consumer monitoring..."
    Write-Host "Monitoring for NEW messages (real-time ML processing)"
    Write-Host "Press Ctrl+C to stop monitoring"
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "pipelines.consumer" -Arguments @("--continuous", "--poll-interval", "5")
}

function KafkaConsumerStream { KafkaConsumerContinuous }

function KafkaCheck {
    Write-Host "Checking native Kafka broker status..."
    if (Test-KafkaBroker) {
        Write-Host "Native Kafka broker is running at localhost:9092"
        Write-Host "Available topics:"
        Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--list")
        Write-Host "Broker information:"
        Invoke-KafkaTool -ToolName "kafka-broker-api-versions" -Arguments @("--bootstrap-server", "localhost:9092")
    } else {
        Write-Host "Cannot connect to native Kafka broker at localhost:9092"
        Write-Host "Please start the broker with kafka-start-bg first"
        Write-Host "Or check installation with kafka-format"
    }
}

function KafkaSampleScored {
    Write-Host "Analyzing churn prediction results..."
    $topics = & (Get-KafkaToolPath -ToolName "kafka-topics") --bootstrap-server localhost:9092 --list 2>$null
    if ($LASTEXITCODE -eq 0 -and ($topics -match "telco\.churn\.predictions")) {
        & (Get-PythonCommand) (Join-Path $ProjectRoot "scripts\kafka_analytics.py")
        if ($LASTEXITCODE -ne 0) { throw "kafka_analytics.py failed" }
    } else {
        Write-Host "telco.churn.predictions topic not found. Run kafka-topics first."
    }
}

function KafkaFlushMessages {
    Write-Host "Flushing all messages from Kafka topics..."
    if (-not (Test-KafkaBroker)) {
        Write-Host "Cannot connect to native Kafka broker"
        Write-Host "Please start the broker with kafka-start-bg first"
        return
    }
    Write-Host "Deleting and recreating topics to flush all messages..."
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--delete", "--topic", "telco.raw.customers")
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--delete", "--topic", "telco.churn.predictions")
    Start-Sleep -Seconds 2
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.raw.customers", "--partitions", "1", "--replication-factor", "1")
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.churn.predictions", "--partitions", "1", "--replication-factor", "1")
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--create", "--topic", "telco.deadletter", "--partitions", "1", "--replication-factor", "1")
    Write-Host "All messages flushed - topics are now empty"
    Invoke-KafkaTool -ToolName "kafka-topics" -Arguments @("--bootstrap-server", "localhost:9092", "--list")
}

function KafkaReset {
    Write-Host "Resetting Kafka data (destructive operation)..."
    $confirm = Read-Host "This will delete all Kafka data. Continue? (y/N)"
    if ($confirm -ne "y") { Write-Host "Kafka reset cancelled."; return }
    Write-Host "Stopping all Kafka processes..."
    Stop-ProcessesByMatch -Pattern "kafka"
    Start-Sleep -Seconds 2
    Write-Host "Force killing port users..."
    Stop-ProcessesOnPorts -Ports @(9092, 9093)
    Start-Sleep -Seconds 1
    Write-Host "Removing Kafka data directory..."
    Remove-PathSafe -Path $KAFKA_LOG_DIR
    Write-Host "Removing PID files..."
    Remove-Item -Path (Join-Path $PID_DIR "kafka.pid") -Force -ErrorAction SilentlyContinue
    Write-Host "Kafka reset completed. Run kafka-format to reinitialize"
}

function KafkaHelp {
    Write-Host "Native Kafka Commands Help"
    Write-Host "=============================================="
    Write-Host "Setup Commands: kafka-format, kafka-start-bg, kafka-stop, kafka-topics"
    Write-Host "Data Commands: kafka-producer-stream, kafka-produce-stream, kafka-producer-batch, kafka-consumer, kafka-consumer-continuous, kafka-consumer-stream"
    Write-Host "Monitoring Commands: kafka-check, kafka-sample-scored, kafka-flush-messages"
    Write-Host "Utility Commands: kafka-reset, kafka-help"
    Write-Host "For detailed setup, see kafka/README.md"
}

function AirflowInit {
    Write-Host "Initializing Apache Airflow..."
    Set-ProjectEnvironment -IncludeWebserverWorkers
    Ensure-AirflowDirectories
    $pythonCommand = Get-PythonCommand
    & $pythonCommand -m pip install "apache-airflow>=2.10.0,<3.0.0"
    if ($LASTEXITCODE -ne 0) { throw "Failed to install apache-airflow" }
    & $pythonCommand -m pip install apache-airflow-providers-apache-spark
    if ($LASTEXITCODE -ne 0) { throw "Failed to install apache-airflow-providers-apache-spark" }
    & $pythonCommand -m airflow db init
    if ($LASTEXITCODE -ne 0) { throw "airflow db init failed" }
    $usersOutput = & $pythonCommand -m airflow users list 2>$null
    if ($usersOutput -notmatch "admin") {
        & $pythonCommand -m airflow users create -u admin -p admin -r Admin -e admin@example.com -f Admin -l User
        if ($LASTEXITCODE -ne 0) { throw "Failed to create Airflow admin user" }
    } else {
        Write-Host "User 'admin' already exists."
    }
    Copy-ProjectDags
    Write-Host "Airflow initialized successfully."
}

function AirflowStart {
    Write-Host "Checking for port conflicts..."
    if (Get-NetTCPConnection -LocalPort @(8080, 8793, 8794) -State Listen -ErrorAction SilentlyContinue) {
        Write-Host "Airflow ports are in use. Cleaning up first..."
        AirflowKill
    }
    Write-Host "Ensuring DAGs are copied..."
    Copy-ProjectDags
    Write-Host "Starting Airflow webserver + scheduler..."
    Write-Host "Webserver will be available at http://localhost:8080"
    Write-Host "Login with: admin / admin"
    $webserverProcess = Start-AirflowWebserverBackground
    try {
        Wait-ForAirflowWebserver -ProcessId $webserverProcess.Id
        Set-ProjectEnvironment -IncludeWebserverWorkers
        Invoke-PythonModule -ModuleName "airflow" -Arguments @("scheduler")
    } finally {
        if ($webserverProcess -and -not $webserverProcess.HasExited) {
            Stop-Process -Id $webserverProcess.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

function AirflowKill {
    Write-Host "Killing all Airflow processes..."
    Stop-ProcessesByMatch -Pattern "airflow"
    Start-Sleep -Seconds 2
    Write-Host "Force killing any remaining Airflow processes..."
    Stop-ProcessesByMatch -Pattern "airflow"
    Start-Sleep -Seconds 1
    Write-Host "Freeing Airflow ports (8080, 8793, 8794)..."
    Stop-ProcessesOnPorts -Ports @(8080, 8793, 8794)
    Start-Sleep -Seconds 1
    Write-Host "Cleaning up PID files..."
    Remove-Item -Path (Join-Path $AIRFLOW_HOME "airflow-webserver.pid") -Force -ErrorAction SilentlyContinue
    Remove-Item -Path (Join-Path $AIRFLOW_HOME "airflow-scheduler.pid") -Force -ErrorAction SilentlyContinue
    Remove-Item -Path (Join-Path $AIRFLOW_HOME "airflow-triggerer.pid") -Force -ErrorAction SilentlyContinue
    Write-Host "All Airflow processes killed and ports freed successfully."
}

function AirflowReset {
    Write-Host "Resetting Airflow database and fixing login issues..."
    AirflowKill
    Write-Host "Removing old database and logs..."
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "airflow.db")
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "logs\*")
    Get-ChildItem -Path $ProjectRoot -Recurse -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue | ForEach-Object { if ($_.FullName -notmatch "\\.venv\\") { Remove-PathSafe -Path $_.FullName } }
    Get-ChildItem -Path $ProjectRoot -Recurse -File -Filter "*.pyc" -ErrorAction SilentlyContinue | ForEach-Object { if ($_.FullName -notmatch "\\.venv\\") { Remove-PathSafe -Path $_.FullName } }
    Write-Host "Reinitializing database..."
    Set-ProjectEnvironment -IncludeWebserverWorkers
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("db", "init")
    Write-Host "Creating admin user..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("users", "create", "-u", "admin", "-f", "Admin", "-l", "User", "-p", "admin", "-r", "Admin", "-e", "admin@example.com")
    Write-Host "Copying DAGs..."
    Copy-ProjectDags
    Write-Host "Airflow reset complete. Login: admin/admin"
}

function AirflowWebserver {
    Write-Host "Starting Airflow webserver on http://localhost:8080..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("webserver", "--port", "8080")
}

function AirflowScheduler {
    Write-Host "Starting Airflow scheduler..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("scheduler")
}

function AirflowStartSeparate {
    Write-Host "Starting Airflow webserver and scheduler..."
    Write-Host "Webserver will be available at http://localhost:8080"
    Write-Host "Login with: admin / admin"
    $webserverProcess = Start-AirflowWebserverBackground
    Wait-ForAirflowWebserver -ProcessId $webserverProcess.Id
    $schedulerProcess = Start-AirflowSchedulerBackground
    Write-Host "Webserver PID: $($webserverProcess.Id)"
    Write-Host "Scheduler PID: $($schedulerProcess.Id)"
    Write-Host "Logs are stored in .airflow/logs/"
}

function AirflowDagsList {
    Write-Host "Listing Airflow DAGs..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "list")
}

function AirflowTestDataPipeline {
    Write-Host "Testing data pipeline DAG..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("tasks", "test", "data_pipeline_dag", "run_data_pipeline", "2025-01-01")
}

function AirflowTestTrainingPipeline {
    Write-Host "Testing training pipeline DAG..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("tasks", "test", "train_pipeline_dag", "run_training_pipeline", "2025-01-01")
}

function AirflowTestInferencePipeline {
    Write-Host "Testing inference pipeline DAG..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("tasks", "test", "inference_pipeline_dag", "run_inference_pipeline", "2025-01-01")
}

function AirflowClean {
    Write-Host "Cleaning Airflow database and logs..."
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "airflow.db")
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "logs\*")
}

function AirflowDeleteDags {
    Write-Host "Stopping Airflow if running..."
    AirflowKill
    Write-Host "Configuring Airflow to hide example DAGs..."
    Ensure-AirflowDirectories
    $configPath = Join-Path $AIRFLOW_HOME "airflow.cfg"
    if (Test-Path $configPath) {
        $configContent = Get-Content $configPath
        if ($configContent -match "load_examples\s*=\s*True") {
            ($configContent -replace "load_examples\s*=\s*True", "load_examples = False") | Set-Content $configPath
        } elseif ($configContent -notmatch "load_examples\s*=\s*False") {
            Add-Content -Path $configPath -Value "load_examples = False"
        }
    }
    Write-Host "Deleting project DAG files..."
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "dags\*")
    Write-Host "All DAGs deleted. Example DAGs will be hidden on next start."
    Write-Host "To re-add your project DAGs, copy dags\*.py into .airflow\dags\"
}

function AirflowTriggerAll {
    Write-Host "Triggering all DAGs..."
    Set-ProjectEnvironment
    Write-Host "Running data pipeline in foreground so logs appear in the terminal..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "data_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "Running training pipeline in foreground so logs appear in the terminal..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "train_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "Running inference pipeline in foreground so logs appear in the terminal..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "inference_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "Running Kafka batch inference DAG in foreground so logs appear in the terminal..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "kafka_batch_consumer_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "Running Kafka streaming inference DAG in foreground so logs appear in the terminal..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "kafka_consumer_streaming_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "All DAG logs were run in the terminal."
}

function AirflowTriggerDataPipeline {
    Write-Host "Running data pipeline DAG in foreground so logs appear in the terminal..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "data_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "data_pipeline_dag completed in the terminal"
}

function AirflowTriggerTrainingPipeline {
    Write-Host "Running training pipeline DAG in foreground so logs appear in the terminal..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "train_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "train_pipeline_dag completed in the terminal"
}

function AirflowTriggerInferencePipeline {
    Write-Host "Running inference pipeline DAG in foreground so logs appear in the terminal..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "inference_pipeline_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "inference_pipeline_dag completed in the terminal"
}

function AirflowTriggerKafkaBatchConsumer {
    Write-Host "Running Kafka batch inference DAG in foreground so logs appear in the terminal..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "test", "kafka_batch_consumer_dag", (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd"))
    Write-Host "kafka_batch_consumer_dag completed in the terminal"
}

function AirflowTriggerKafkaConsumerStreaming {
    Write-Host "Running Kafka streaming inference DAG in foreground so logs appear in the terminal..."
    Set-ProjectEnvironment
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("dags", "trigger", "kafka_consumer_streaming_dag")
    Write-Host "kafka_consumer_streaming_dag triggered in Airflow UI"
}

function AirflowHealth {
    Write-Host "Checking Airflow health status..."
    try {
        Invoke-RestMethod -Uri "http://localhost:8080/health" -Method Get -TimeoutSec 5 | ConvertTo-Json -Depth 5
    } catch {
        Write-Host "Airflow not responding"
    }
    Write-Host ""
    Write-Host "Checking running processes..."
    $airflowProcesses = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -and $_.CommandLine -match "airflow" }
    if ($airflowProcesses) {
        $airflowProcesses | Select-Object ProcessId, Name, CommandLine | Format-Table -AutoSize
    } else {
        Write-Host "No Airflow processes found"
    }
}

function ReRunAll {
    Write-Host "Starting complete system reset and restart..."
    Write-Host "Step 1/6: Killing all Airflow processes..."
    AirflowKill
    Write-Host ""
    Write-Host "Step 2/6: Cleaning database, logs, and Python cache files..."
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "airflow.db")
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "logs\*")
    Remove-PathSafe -Path (Join-Path $AIRFLOW_HOME "dags\*")
    Get-ChildItem -Path $ProjectRoot -Recurse -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue | ForEach-Object { if ($_.FullName -notmatch "\\.venv\\") { Remove-PathSafe -Path $_.FullName } }
    Get-ChildItem -Path $ProjectRoot -Recurse -File -Filter "*.pyc" -ErrorAction SilentlyContinue | ForEach-Object { if ($_.FullName -notmatch "\\.venv\\") { Remove-PathSafe -Path $_.FullName } }
    Write-Host "Database, logs, and Python cache files cleaned"
    Write-Host ""
    Write-Host "Step 3/6: Reinitializing Airflow database..."
    Set-ProjectEnvironment -IncludeWebserverWorkers
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("db", "migrate")
    Write-Host "Database reinitialized"
    Write-Host ""
    Write-Host "Step 4/6: Creating admin user..."
    Invoke-PythonModule -ModuleName "airflow" -Arguments @("users", "create", "-u", "admin", "-f", "Admin", "-l", "User", "-p", "admin", "-r", "Admin", "-e", "admin@example.com")
    Write-Host "Admin user ready (admin/admin)"
    Write-Host ""
    Write-Host "Step 5/6: Copying fresh DAGs..."
    Copy-ProjectDags
    Write-Host "DAGs copied"
    Write-Host ""
    Write-Host "Step 6/6: Starting Airflow with the stable webserver + scheduler flow..."
    Write-Host "Starting Airflow webserver and scheduler..."
    Write-Host "Webserver will be available at http://localhost:8080"
    Write-Host "Login with: admin / admin"
    AirflowStart
    Write-Host ""
    Write-Host "Complete reset and restart finished"
    Write-Host "Web UI: http://localhost:8080"
    Write-Host "Login: admin / admin"
    Write-Host "Scheduling: data pipeline, training pipeline, inference pipeline, Kafka consumers"
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
    "kafka-format" { KafkaFormat }
    "kafka-start-bg" { KafkaStartBg }
    "kafka-stop" { KafkaStop }
    "kafka-topics" { KafkaTopics }
    "kafka-producer-stream" { KafkaProducerStream }
    "kafka-produce-stream" { KafkaProducerStream }
    "kafka-producer-batch" { KafkaProducerBatch }
    "kafka-consumer" { KafkaConsumer }
    "kafka-consumer-continuous" { KafkaConsumerContinuous }
    "kafka-consumer-stream" { KafkaConsumerContinuous }
    "kafka-check" { KafkaCheck }
    "kafka-sample-scored" { KafkaSampleScored }
    "kafka-flush-messages" { KafkaFlushMessages }
    "kafka-reset" { KafkaReset }
    "kafka-help" { KafkaHelp }
    "airflow-init" { AirflowInit }
    "airflow-start" { AirflowStart }
    "airflow-kill" { AirflowKill }
    "airflow-reset" { AirflowReset }
    "airflow-webserver" { AirflowWebserver }
    "airflow-scheduler" { AirflowScheduler }
    "airflow-start-separate" { AirflowStartSeparate }
    "airflow-dags-list" { AirflowDagsList }
    "airflow-test-data-pipeline" { AirflowTestDataPipeline }
    "airflow-test-training-pipeline" { AirflowTestTrainingPipeline }
    "airflow-test-inference-pipeline" { AirflowTestInferencePipeline }
    "airflow-clean" { AirflowClean }
    "airflow-delete-dags" { AirflowDeleteDags }
    "airflow-trigger-all" { AirflowTriggerAll }
    "airflow-trigger-data-pipeline" { AirflowTriggerDataPipeline }
    "airflow-trigger-training-pipeline" { AirflowTriggerTrainingPipeline }
    "airflow-trigger-inference-pipeline" { AirflowTriggerInferencePipeline }
    "airflow-trigger-kafka-batch-consumer" { AirflowTriggerKafkaBatchConsumer }
    "airflow-trigger-kafka-consumer-streaming" { AirflowTriggerKafkaConsumerStreaming }
    "airflow-health" { AirflowHealth }
    "re-run-all" { ReRunAll }
    default { Help }
}