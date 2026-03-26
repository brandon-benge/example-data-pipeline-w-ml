FROM flink:1.20.3-scala_2.12-java17

USER root

COPY flink/requirements.txt /tmp/flink-requirements.txt
COPY flink /opt/flink/usrlib/repo/flink
COPY config/features /opt/flink/usrlib/repo/config/features
COPY generator/schemas /opt/flink/usrlib/repo/generator/schemas
COPY config/flink/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY config/flink/start-flink.sh /opt/flink/bootstrap/start-flink.sh
COPY config/flink/submit-jobs.sh /opt/flink/bootstrap/submit-jobs.sh

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
    && mkdir -p /opt/flink/usrlib/repo/flink/.deps \
    && python3 -m pip install --no-cache-dir --target /opt/flink/usrlib/repo/flink/.deps -r /tmp/flink-requirements.txt \
    && mkdir -p /opt/flink/usrlib/repo/flink/jars \
    && curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar" -o /opt/flink/usrlib/repo/flink/jars/iceberg-flink-runtime-1.20-1.10.1.jar \
    && curl --retry 5 --retry-all-errors --retry-delay 2 -fsSL "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" -o /opt/flink/usrlib/repo/flink/jars/flink-sql-connector-kafka-3.3.0-1.20.jar \
    && rm -rf /var/lib/apt/lists/*

RUN chmod +x /opt/flink/bootstrap/start-flink.sh /opt/flink/bootstrap/submit-jobs.sh \
    && rm -f /tmp/flink-requirements.txt

USER flink
