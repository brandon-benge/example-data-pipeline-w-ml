# Debezium Transform Debug Helper

This repo includes a small helper for inspecting what the Iceberg Kafka Connect SMT chain produces for a CDC topic before the Iceberg write path runs.

Files:

- [tools/print_debezium_transform.sh](../../tools/print_debezium_transform.sh)
- [tools/PrintDebeziumTransform.java](../../tools/PrintDebeziumTransform.java)

## What it does

The helper:

1. reads one or more records from a Kafka topic
2. deserializes them with Kafka Connect `JsonConverter`
3. applies the same `DebeziumTransform` used by the Iceberg sink
4. applies the same `KafkaMetadataTransform` used by the Iceberg sink
5. prints the record at each stage
6. generates a Trino `INSERT` for the transformed row

Output sections:

- `=== Raw Envelope ===`
- `=== After DebeziumTransform ===`
- `=== After KafkaMetadataTransform ===`
- `=== Trino Insert ===`

## Usage

Print one transformed record:

```bash
./tools/print_debezium_transform.sh cdc.order_header
```

Print multiple transformed records:

```bash
./tools/print_debezium_transform.sh cdc.customer_session 3
```

Insert one transformed record directly into the matching Bronze Iceberg table through Trino:

```bash
./tools/print_debezium_transform.sh cdc.order_header --insert-trino
```

## Topic argument

Pass the Kafka topic name directly.

For `cdc.*` topics, the helper infers the same target pattern convention used by the Bronze CDC sinks:

- `cdc.order_header` -> `bronze.bronze_order_header_cdc`
- `cdc.customer_session` -> `bronze.bronze_customer_session_cdc`

## Why this exists

This is useful when a sink connector is `RUNNING` but no Iceberg rows land, and you need to know whether the record shape after SMT processing actually matches the Bronze table contract.

## Important limitations

- This tool validates the SMT output shape, not the Iceberg commit path.
- It runs outside the actual Kafka Connect worker task lifecycle, so it does not reproduce commit/coordinator issues.
- `_kafka_metadata_timestamp` may appear `null` in this helper because the debug `SinkRecord` does not carry broker timestamp metadata the same way the live sink task does.
- `--insert-trino` tests whether the transformed row can be written through Trino into the current Bronze table contract. It does not reproduce the Kafka Connect sink coordinator path.

## Current debugging value

For `cdc.order_header`, this helper already showed that:

- `DebeziumTransform` unwraps `payload.after` into top-level business fields
- it adds `_cdc`
- `KafkaMetadataTransform` adds `_kafka_metadata_*`
- the generated `INSERT` can be executed successfully through Trino into `iceberg.bronze.bronze_order_header_cdc`

That makes it possible to compare the transformed output directly against:

- [config/iceberg/bootstrap-cdc-rest-catalog.sql](../../config/iceberg/bootstrap-cdc-rest-catalog.sql)

which is the actual Bronze table contract.

Current implication:

- the transformed `order_header` row is acceptable to the Bronze Iceberg table contract through Trino
- so a failure in the Kafka Connect Iceberg sink path is more likely to be in sink runtime / coordination behavior than in the transformed row values alone
