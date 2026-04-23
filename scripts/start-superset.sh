#!/bin/bash
set -e

echo "Running Superset migrations..."
superset db upgrade

echo "Ensuring admin user exists..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin || true

echo "Initializing Superset..."
superset init

echo "Registering TimescaleDB connection..."
superset set-database-uri \
  -d "${SUPERSET_BI_DB_NAME:-Ice Monitor Timescale}" \
  -u "${SUPERSET_BI_URI:-postgresql://postgres:ice@db:5432/ice_monitor}"

echo "Waiting for TimescaleDB DNS and port..."
python - <<'PY'
import socket
import sys
import time

host = "db"
port = 5432

for attempt in range(60):
    try:
        ip = socket.gethostbyname(host)
        with socket.create_connection((host, port), timeout=2):
            print(f"TimescaleDB is reachable at {ip}:{port}", flush=True)
            sys.exit(0)
    except OSError as exc:
        print(f"Waiting for TimescaleDB ({attempt + 1}/60): {exc}", flush=True)
        time.sleep(2)

raise SystemExit("TimescaleDB did not become reachable in time")
PY

echo "Waiting for model metrics tables..."
python - <<'PY'
import os
import sys
import time

from sqlalchemy import create_engine, text

uri = os.environ.get("SUPERSET_BI_URI", "postgresql://postgres:ice@db:5432/ice_monitor")
engine = create_engine(uri, pool_pre_ping=True)

required_tables = {"ice_metrics", "ice_system_metrics"}
tables_sql = ", ".join(f"'{name}'" for name in sorted(required_tables))

for attempt in range(90):
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name IN ({tables_sql})
                    """
                ),
            ).fetchall()
            present = {row[0] for row in rows}
            if required_tables.issubset(present):
                print(f"Metrics tables are ready: {sorted(present)}", flush=True)
                sys.exit(0)
    except Exception as exc:
        print(f"Waiting for metrics tables ({attempt + 1}/90): {exc}", flush=True)

    time.sleep(2)

raise SystemExit("Metrics tables did not become available in time")
PY

echo "Bootstrapping Superset datasets..."
python /opt/ice-monitor/scripts/bootstrap_superset_content.py

exec superset run -h 0.0.0.0 -p 8088 --with-threads
