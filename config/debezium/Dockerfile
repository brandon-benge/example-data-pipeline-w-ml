FROM gradle:8.10.2-jdk17 AS iceberg_connect_builder

ARG ICEBERG_VERSION=1.9.1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates tar gzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl --retry 5 --retry-all-errors --retry-delay 2 -fSL \
    -o /tmp/apache-iceberg-src.tar.gz \
    "https://downloads.apache.org/iceberg/apache-iceberg-${ICEBERG_VERSION}/apache-iceberg-${ICEBERG_VERSION}.tar.gz" \
    && tar -xzf /tmp/apache-iceberg-src.tar.gz --strip-components=1 -C /build \
    && rm -f /tmp/apache-iceberg-src.tar.gz

# Build the official Apache Iceberg Kafka Connect runtime distribution.
RUN ./gradlew --no-daemon -x test -x integrationTest clean build

RUN set -eux; \
    artifact="$(find /build/kafka-connect/kafka-connect-runtime/build/distributions -maxdepth 1 \( -name '*.zip' -o -name '*.tar' -o -name '*.tgz' \) | head -1)"; \
    test -n "$artifact"; \
    mkdir -p /opt/iceberg-kafka-connect; \
    case "$artifact" in \
      *.zip) unzip -q "$artifact" -d /tmp/iceberg-connect ;; \
      *.tar|*.tgz) mkdir -p /tmp/iceberg-connect && tar -xf "$artifact" -C /tmp/iceberg-connect ;; \
    esac; \
    dist_dir="$(find /tmp/iceberg-connect -maxdepth 2 -type d -name 'iceberg-kafka-connect-runtime*' | head -1)"; \
    test -n "$dist_dir"; \
    cp -R "$dist_dir"/lib/* /opt/iceberg-kafka-connect/; \
    rm -rf /tmp/iceberg-connect


FROM debezium/connect:2.7.3.Final

USER root

RUN microdnf install -y tar gzip unzip findutils \
    && microdnf clean all

COPY --from=iceberg_connect_builder /opt/iceberg-kafka-connect/ /kafka/connect/iceberg/

ENV CONNECT_PLUGIN_PATH=/kafka/connect

USER kafka
