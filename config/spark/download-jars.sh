#!/bin/sh
set -eu

mkdir -p /app/spark/jars

download() {
  url="$1"
  output="$2"
  if [ ! -f "$output" ]; then
    curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "$url" -o "$output"
  fi
}

download \
  "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" \
  "/app/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"

download \
  "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar" \
  "/app/spark/jars/hadoop-aws-3.4.1.jar"

download \
  "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar" \
  "/app/spark/jars/aws-java-sdk-bundle-1.12.780.jar"

download \
  "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" \
  "/app/spark/jars/iceberg-aws-bundle-1.10.1.jar"

echo "Spark runtime jars are available."
