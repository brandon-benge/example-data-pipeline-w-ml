from __future__ import annotations

import atexit
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from generator.config import GeneratorSettings

ROOT = Path(__file__).resolve().parent.parent
PORT_FORWARD_PROCESSES: list[subprocess.Popen[str]] = []


class HostAccessError(RuntimeError):
    pass


def _cleanup_port_forwards() -> None:
    for process in PORT_FORWARD_PROCESSES:
        if process.poll() is None:
            process.terminate()
    for process in PORT_FORWARD_PROCESSES:
        if process.poll() is None:
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()


atexit.register(_cleanup_port_forwards)


def _is_local_target(host: str) -> bool:
    return host in {"localhost", "127.0.0.1"}


def _wait_for_tcp(host: str, port: int, *, timeout_seconds: int = 10) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def _wait_for_http(url: str, *, timeout_seconds: int = 10) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return 200 <= response.status < 500
        except urllib.error.URLError:
            time.sleep(0.5)
    return False


def _start_port_forward(*, namespace: str, service: str, local_port: int, remote_port: int) -> None:
    log_path = ROOT / f".tmp-port-forward-{service}-{local_port}.log"
    log_handle = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            "kubectl",
            "-n",
            namespace,
            "port-forward",
            f"svc/{service}",
            f"{local_port}:{remote_port}",
        ],
        cwd=ROOT,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    PORT_FORWARD_PROCESSES.append(process)


def _ensure_local_service(
    *,
    host: str,
    port: int,
    namespace: str,
    service: str,
    remote_port: int,
    description: str,
    healthcheck_url: str | None = None,
) -> None:
    if not _is_local_target(host):
        if _wait_for_tcp(host, port):
            return
        raise HostAccessError(f"{description} is not reachable at {host}:{port}")

    if _wait_for_tcp(host, port) and (healthcheck_url is None or _wait_for_http(healthcheck_url)):
        return

    _start_port_forward(
        namespace=namespace,
        service=service,
        local_port=port,
        remote_port=remote_port,
    )

    if not _wait_for_tcp(host, port):
        raise HostAccessError(
            f"Failed to open localhost:{port} for {description}. Start `kubectl port-forward svc/{service} {port}:{remote_port} -n {namespace}`."
        )

    if healthcheck_url is not None and not _wait_for_http(healthcheck_url):
        raise HostAccessError(
            f"Opened localhost:{port} for {description}, but the service did not become healthy at {healthcheck_url}."
        )


def ensure_generator_access(settings: GeneratorSettings, mode: str) -> None:
    if mode in {"postgres", "both"}:
        _ensure_local_service(
            host=settings.postgres_host,
            port=settings.postgres_port,
            namespace="data-platform-infra",
            service="postgres",
            remote_port=5432,
            description="Postgres",
        )

    if mode in {"kafka", "both"}:
        bootstrap_server = settings.kafka_bootstrap_servers.split(",", 1)[0].strip()
        if ":" not in bootstrap_server:
            raise HostAccessError(f"Invalid Kafka bootstrap server: {bootstrap_server}")
        kafka_host, kafka_port_text = bootstrap_server.rsplit(":", 1)
        kafka_port = int(kafka_port_text)
        _ensure_local_service(
            host=kafka_host,
            port=kafka_port,
            namespace="data-platform-infra",
            service="kafka",
            remote_port=kafka_port,
            description="Kafka",
        )

        schema_registry = urlparse(settings.schema_registry_url)
        if not schema_registry.hostname or schema_registry.port is None:
            raise HostAccessError(f"Invalid schema registry URL: {settings.schema_registry_url}")
        _ensure_local_service(
            host=schema_registry.hostname,
            port=schema_registry.port,
            namespace="data-platform-infra",
            service="schema-registry",
            remote_port=8081,
            description="Schema Registry",
            healthcheck_url=f"{settings.schema_registry_url.rstrip('/')}/subjects",
        )
