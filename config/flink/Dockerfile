FROM flink:1.20.3-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 python3-pip \
    && python3 -m pip install --no-cache-dir apache-beam==2.54.0 \
    && python3 -m pip install --no-cache-dir grpcio avro-python3 \
    && mkdir -p /opt/flink/opt/python/pyflink_pkg \
    && python3 -c "import zipfile; zipfile.ZipFile('/opt/flink/opt/python/pyflink.zip').extractall('/opt/flink/opt/python/pyflink_pkg')" \
    && chmod +x /opt/flink/opt/python/pyflink_pkg/pyflink/bin/*.sh \
    && curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar" -o /opt/flink/lib/hadoop-client-api-3.4.1.jar \
    && curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar" -o /opt/flink/lib/hadoop-client-runtime-3.4.1.jar \
    && curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" -o /opt/flink/lib/iceberg-aws-bundle-1.10.1.jar \
    && rm -rf /var/lib/apt/lists/*

USER flink
