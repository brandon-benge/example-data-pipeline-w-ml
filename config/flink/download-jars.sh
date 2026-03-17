#!/bin/sh
set -eu

mkdir -p /opt/flink/usrlib/repo/flink/jars

download() {
  url="$1"
  output="$2"
  if [ ! -f "$output" ]; then
    curl -fsSL "$url" -o "$output"
  fi
}

download \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar" \
  "/opt/flink/usrlib/repo/flink/jars/iceberg-flink-runtime-1.20-1.10.1.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" \
  "/opt/flink/usrlib/repo/flink/jars/flink-sql-connector-kafka-3.3.0-1.20.jar"

download \
  "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar" \
  "/opt/flink/usrlib/repo/flink/jars/hadoop-client-api-3.4.1.jar"

download \
  "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar" \
  "/opt/flink/usrlib/repo/flink/jars/hadoop-client-runtime-3.4.1.jar"

download \
  "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" \
  "/opt/flink/usrlib/repo/flink/jars/iceberg-aws-bundle-1.10.1.jar"

echo "Flink connector jars are available."
