from __future__ import annotations

import io
import json
import struct
from pathlib import Path
from typing import Any

try:
    import requests
    from fastavro import parse_schema, schemaless_writer
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover - dependency guard
    requests = None
    parse_schema = None
    schemaless_writer = None
    KafkaProducer = None


class SchemaRegistryError(RuntimeError):
    pass


class SessionEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        schema_path: str | Path,
    ) -> None:
        require_kafka_dependencies()
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url.rstrip("/")
        self.topic = topic
        self.value_schema = self._load_schema(schema_path)
        self.parsed_value_schema = parse_schema(self.value_schema)
        self.key_schema = {
            "type": "record",
            "name": "session_event_key",
            "namespace": "events",
            "fields": [{"name": "event_uuid", "type": "string"}],
        }
        self.parsed_key_schema = parse_schema(self.key_schema)
        self.value_subject = f"{topic}-value"
        self.key_subject = f"{topic}-key"
        self.value_schema_id = self._ensure_subject(self.value_subject, self.value_schema)
        self.key_schema_id = self._ensure_subject(self.key_subject, self.key_schema)
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            compression_type="lz4",
            linger_ms=10,
            retries=5,
            max_in_flight_requests_per_connection=1,
        )

    def _load_schema(self, schema_path: str | Path) -> dict[str, Any]:
        with Path(schema_path).open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _ensure_subject(self, subject: str, schema: dict[str, Any]) -> int:
        latest_url = f"{self.schema_registry_url}/subjects/{subject}/versions/latest"
        response = requests.get(latest_url, timeout=10)
        serialized_schema = json.dumps(schema, separators=(",", ":"))
        if response.status_code == 200:
            payload = response.json()
            if json.loads(payload["schema"]) == schema:
                return int(payload["id"])
        register_response = requests.post(
            f"{self.schema_registry_url}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json={"schemaType": "AVRO", "schema": serialized_schema},
            timeout=10,
        )
        if register_response.status_code not in (200, 201):
            raise SchemaRegistryError(
                f"Failed to register schema for {subject}: {register_response.status_code} {register_response.text}"
            )
        return int(register_response.json()["id"])

    def _encode(self, schema_id: int, schema: dict[str, Any], payload: dict[str, Any]) -> bytes:
        buffer = io.BytesIO()
        buffer.write(struct.pack(">bI", 0, schema_id))
        parsed_schema = self.parsed_key_schema if schema is self.key_schema else self.parsed_value_schema
        schemaless_writer(buffer, parsed_schema, payload)
        return buffer.getvalue()

    def publish(self, events: list[dict[str, Any]]) -> int:
        total = len(events)
        progress_every = max(1, min(10_000, total // 10 if total > 0 else 1))
        for index, event in enumerate(events, start=1):
            key_payload = {"event_uuid": event["event_uuid"]}
            key_bytes = self._encode(self.key_schema_id, self.key_schema, key_payload)
            value_bytes = self._encode(self.value_schema_id, self.value_schema, event)
            self.producer.send(self.topic, key=key_bytes, value=value_bytes)
            if index == 1 or index % progress_every == 0 or index == total:
                print(f"[kafka] queued {index}/{total} events", flush=True)
        print("[kafka] flushing producer", flush=True)
        self.producer.flush()
        print("[kafka] flush complete", flush=True)
        return len(events)


def require_kafka_dependencies() -> None:
    if None in {requests, parse_schema, schemaless_writer, KafkaProducer}:
        raise RuntimeError(
            "Kafka output requires requests, fastavro, kafka-python, and lz4. Install generator dependencies first."
        )
