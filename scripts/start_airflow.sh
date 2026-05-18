#!/usr/bin/env bash
set -euo pipefail

echo "Starting Airflow webserver + scheduler via script"
export AIRFLOW_HOME="$(pwd)/.airflow"
export PATH="$(pwd)/.venv/bin:$PATH"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__WORKERS=1
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"
export PYTHONWARNINGS="ignore::DeprecationWarning"

mkdir -p "$AIRFLOW_HOME" "$AIRFLOW_HOME/dags" "$AIRFLOW_HOME/logs"

if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
  echo "Initializing Airflow metadata database..."
  .venv/bin/airflow db init
else
  echo "Upgrading Airflow metadata database..."
  .venv/bin/airflow db migrate
fi

echo "Ensuring admin user exists (idempotent)..."
.venv/bin/airflow users create -u admin -f Admin -l User -p admin -r Admin -e admin@example.com 2>/dev/null || true

trap "kill 0" INT TERM EXIT

echo "Starting webserver (debug mode on macOS to avoid Gunicorn fork issues)"
.venv/bin/airflow webserver --port 8080 --debug &
webserver_pid=$!

until curl -fs http://localhost:8080/ >/dev/null 2>&1; do
  if ! kill -0 "$webserver_pid" >/dev/null 2>&1; then
    echo "Webserver died before becoming healthy" >&2
    exit 1
  fi
  sleep 1
done

echo "Starting scheduler after webserver and DB are ready"
.venv/bin/airflow scheduler
