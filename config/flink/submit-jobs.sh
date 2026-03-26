#!/bin/sh
set -eu

export PYTHONPATH="${PYTHONPATH:-/opt/flink/usrlib/repo/flink:/opt/flink/usrlib/repo:/opt/flink/usrlib/repo/flink/.deps}"

wait_for_jobmanager() {
  python3 - <<'PY'
import sys
import time
import urllib.request

url = "http://flink-jobmanager:8081/overview"
for _ in range(60):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            if response.status == 200:
                sys.exit(0)
    except Exception:
        time.sleep(2)
sys.exit(1)
PY
}

job_running() {
  job_name="$1"
  python3 - "$job_name" <<'PY'
import json
import sys
import urllib.request

job_name = sys.argv[1]
with urllib.request.urlopen("http://flink-jobmanager:8081/jobs/overview", timeout=5) as response:
    payload = json.load(response)

for job in payload.get("jobs", []):
    if job.get("name") != job_name:
        continue

    if job.get("state") in {
        "RUNNING",
        "CREATED",
        "INITIALIZING",
        "DEPLOYING",
        "RESTARTING",
        "RECONCILING",
        "SUSPENDED",
    }:
        sys.exit(0)

sys.exit(1)
PY
}

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <job-name> <python-module>" >&2
  exit 1
fi

submit_job() {
  job_name="$1"
  module_name="$2"
  parallelism="${FLINK_JOB_PARALLELISM:-4}"

  set -- /opt/flink/bin/flink run \
    --detached \
    --target remote \
    -Djobmanager.rpc.address=flink-jobmanager \
    -Drest.address=flink-jobmanager \
    -Drest.port=8081 \
    -pyclientexec python3 \
    -pyexec python3 \
    -pyfs /opt/flink/usrlib/repo,/opt/flink/usrlib/repo/flink/.deps \
    -p "$parallelism"

  if [ -n "${FLINK_PIPELINE_JARS:-}" ]; then
    old_ifs="$IFS"
    IFS=';'
    for jar_uri in $FLINK_PIPELINE_JARS; do
      if [ -n "$jar_uri" ]; then
        set -- "$@" -C "$jar_uri"
      fi
    done
    IFS="$old_ifs"
  fi

  set -- "$@" -pym "$module_name"
  "$@"
  echo "Submitted Flink job: $job_name"
}

job_name="$1"
module_name="$2"
poll_seconds="${FLINK_JOB_POLL_SECONDS:-15}"
retry_seconds="${FLINK_SUBMIT_RETRY_SECONDS:-120}"

while true; do
  wait_for_jobmanager

  if job_running "$job_name"; then
    echo "Flink job running: $job_name"
  else
    echo "Flink job missing, submitting: $job_name"
    if submit_job "$job_name" "$module_name"; then
      :
    else
      echo "Flink job submission failed; retrying in ${retry_seconds}s" >&2
      sleep "$retry_seconds"
      continue
    fi
  fi

  sleep "$poll_seconds"
done
