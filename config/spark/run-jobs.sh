#!/bin/sh
set -eu

export PYTHONPATH="${PYTHONPATH:-/app}"

wait_for_endpoint() {
  url="$1"
  python3 - "$url" <<'PY'
import sys
import time
import urllib.request
import urllib.error

url = sys.argv[1]
for _ in range(60):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            if response.status < 500:
                sys.exit(0)
    except urllib.error.HTTPError as exc:
        if exc.code < 500:
            sys.exit(0)
        time.sleep(5)
    except Exception:
        time.sleep(5)
sys.exit(1)
PY
}

wait_for_endpoint "http://iceberg-rest.data-platform-infra:8181"
exec /opt/spark/bin/spark-submit /app/spark/jobs/scheduler.py
