#!/bin/sh
set -eu

mkdir -p /tmp/flink-conf /tmp/flink-tmp /opt/flink/state/checkpoints /opt/flink/state/savepoints
cp -R /opt/flink/conf/. /tmp/flink-conf/
cp /opt/flink/usrlib/repo/config/flink/flink-conf.yaml /tmp/flink-conf/flink-conf.yaml
export FLINK_CONF_DIR=/tmp/flink-conf
export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.net=ALL-UNNAMED -Djava.io.tmpdir=/tmp/flink-tmp ${JAVA_TOOL_OPTIONS:-}"

extra_classpath=""
for jar in /opt/flink/usrlib/repo/flink/jars/*.jar; do
  if [ -f "$jar" ]; then
    target="/opt/flink/lib/$(basename "$jar")"
    if [ ! -e "$target" ]; then
      ln -sf "$jar" "$target"
    fi
    if [ -n "$extra_classpath" ]; then
      extra_classpath="${extra_classpath}:"
    fi
    extra_classpath="${extra_classpath}${jar}"
  fi
done

if [ -n "$extra_classpath" ]; then
  export FLINK_CLASSPATH="${extra_classpath}${FLINK_CLASSPATH:+:${FLINK_CLASSPATH}}"
fi

chmod 1777 /tmp /tmp/flink-tmp
chmod -R 777 /tmp/flink-conf /opt/flink/state

if ! command -v python3 >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends python3 python3-pip
  rm -rf /var/lib/apt/lists/*
fi

if [ ! -x /usr/bin/python ]; then
  ln -sf /usr/bin/python3 /usr/bin/python
fi

if [ ! -d /opt/flink/opt/python/pyflink_pkg ] && [ -f /opt/flink/opt/python/pyflink.zip ]; then
  mkdir -p /opt/flink/opt/python/pyflink_pkg
  python3 - <<'PY'
import zipfile

with zipfile.ZipFile("/opt/flink/opt/python/pyflink.zip") as archive:
    archive.extractall("/opt/flink/opt/python/pyflink_pkg")
PY
fi

if [ -d /opt/flink/opt/python/pyflink_pkg/pyflink/bin ]; then
  chmod +x /opt/flink/opt/python/pyflink_pkg/pyflink/bin/*.sh
fi

exec /bin/sh "$@"
