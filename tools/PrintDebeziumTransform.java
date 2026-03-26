import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.iceberg.connect.transforms.DebeziumTransform;
import org.apache.iceberg.connect.transforms.KafkaMetadataTransform;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class PrintDebeziumTransform {
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
  private static final SimpleDateFormat TIMESTAMP_FORMAT = timestampFormatter();

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java PrintDebeziumTransform <topic> [max_messages] [--sql-only]");
      System.exit(1);
    }

    String topic = args[0];
    int maxMessages = 1;
    boolean sqlOnly = false;

    for (int i = 1; i < args.length; i += 1) {
      if ("--sql-only".equals(args[i])) {
        sqlOnly = true;
      } else {
        maxMessages = Integer.parseInt(args[i]);
      }
    }

    String targetPattern = inferTargetPattern(topic);

    JsonConverter keyConverter = jsonConverter();
    JsonConverter valueConverter = jsonConverter();

    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "kafka.data-platform-infra:9092");
    consumerProps.put("group.id", "debug-print-debezium-transform-" + System.currentTimeMillis());
    consumerProps.put("auto.offset.reset", "earliest");
    consumerProps.put("enable.auto.commit", "false");
    consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
    consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singletonList(topic));

      DebeziumTransform<SinkRecord> debeziumTransform = new DebeziumTransform<>();
      KafkaMetadataTransform kafkaMetadataTransform = new KafkaMetadataTransform();

      debeziumTransform.configure(Map.of("cdc.target.pattern", targetPattern));
      kafkaMetadataTransform.configure(Map.of("nested", "false"));

      int printed = 0;
      while (printed < maxMessages) {
        boolean sawRecord = false;
        for (ConsumerRecord<byte[], byte[]> raw : consumer.poll(POLL_TIMEOUT)) {
          sawRecord = true;
          SinkRecord record = toSinkRecord(raw, keyConverter, valueConverter);
          SinkRecord afterDebezium = applyTransform(debeziumTransform, record, "DebeziumTransform");
          SinkRecord afterKafkaMeta = applyTransform(kafkaMetadataTransform, afterDebezium, "KafkaMetadataTransform");
          String insertSql = buildInsertSql(targetPattern, afterKafkaMeta);

          if (sqlOnly) {
            System.out.println(insertSql);
          } else {
            System.out.println("=== Raw Envelope ===");
            printRecord(valueConverter, record);

            System.out.println("=== After DebeziumTransform ===");
            printRecord(valueConverter, afterDebezium);

            System.out.println("=== After KafkaMetadataTransform ===");
            printRecord(valueConverter, afterKafkaMeta);

            System.out.println("=== Trino Insert ===");
            System.out.println(insertSql);
          }

          printed += 1;
          if (printed >= maxMessages) {
            break;
          }
        }

        if (!sawRecord) {
          System.err.println("No records found on topic: " + topic);
          System.exit(2);
        }
      }
    }
  }

  private static SinkRecord applyTransform(Transformation<SinkRecord> transform, SinkRecord record, String name) {
    SinkRecord transformed = transform.apply(record);
    if (transformed == null) {
      throw new IllegalStateException(name + " returned null");
    }
    return transformed;
  }

  private static SinkRecord toSinkRecord(
      ConsumerRecord<byte[], byte[]> raw,
      JsonConverter keyConverter,
      JsonConverter valueConverter) {
    var keySchemaAndValue = keyConverter.toConnectData(raw.topic(), raw.headers(), raw.key());
    var valueSchemaAndValue = valueConverter.toConnectData(raw.topic(), raw.headers(), raw.value());

    return new SinkRecord(
        raw.topic(),
        raw.partition(),
        keySchemaAndValue.schema(),
        keySchemaAndValue.value(),
        valueSchemaAndValue.schema(),
        valueSchemaAndValue.value(),
        raw.offset(),
        raw.timestamp(),
        raw.timestampType());
  }

  private static JsonConverter jsonConverter() {
    JsonConverter converter = new JsonConverter();
    Map<String, Object> props = new HashMap<>();
    props.put(ConverterConfig.TYPE_CONFIG, "value");
    props.put("schemas.enable", "true");
    converter.configure(props, false);
    return converter;
  }

  private static void printRecord(JsonConverter converter, SinkRecord record) {
    byte[] serialized = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    System.out.println(new String(serialized, StandardCharsets.UTF_8));
  }

  private static String inferTargetPattern(String topic) {
    if (topic.startsWith("cdc.")) {
      return "bronze.bronze_" + topic.substring(4) + "_cdc";
    }
    return topic;
  }

  private static String buildInsertSql(String targetTable, SinkRecord record) {
    Struct value = (Struct) record.value();
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    for (Field field : value.schema().fields()) {
      if (columns.length() > 0) {
        columns.append(", ");
        values.append(", ");
      }
      columns.append(field.name());
      if ("_cdc".equals(field.name())) {
        values.append(cdcToSql((Struct) value.get(field)));
      } else {
        values.append(toSqlLiteral(field.name(), field.schema(), value.get(field)));
      }
    }

    return "INSERT INTO iceberg." + targetTable + " (" + columns + ") VALUES (" + values + ")";
  }

  private static String toSqlLiteral(String fieldName, Schema schema, Object value) {
    if (value == null) {
      return "NULL";
    }

    String schemaName = schema.name();
    if (Decimal.LOGICAL_NAME.equals(schemaName)) {
      return "CAST(DECIMAL '" + ((BigDecimal) value).toPlainString() + "' AS " + trinoType(schema) + ")";
    }
    if ("org.apache.kafka.connect.data.Timestamp".equals(schemaName)) {
      return "CAST(TIMESTAMP '" + TIMESTAMP_FORMAT.format((Date) value) + "' AS TIMESTAMP(6))";
    }
    if ("_kafka_metadata_timestamp".equals(fieldName)) {
      long epochMillis = ((Number) value).longValue();
      return "CAST(from_unixtime(" + epochMillis + " / 1000.0) AS TIMESTAMP(6))";
    }

    return switch (schema.type()) {
      case INT8, INT16, INT32 -> "CAST(" + value + " AS INTEGER)";
      case INT64 -> "CAST(" + value + " AS BIGINT)";
      case FLOAT32 -> "CAST(" + value + " AS REAL)";
      case FLOAT64 -> "CAST(" + value + " AS DOUBLE)";
      case BOOLEAN -> ((Boolean) value) ? "true" : "false";
      case STRING -> "CAST('" + escapeSql(value.toString()) + "' AS VARCHAR)";
      case BYTES -> "X'" + bytesToHex((byte[]) value) + "'";
      case STRUCT -> structToSql(schema, (Struct) value);
      default -> "CAST('" + escapeSql(value.toString()) + "' AS VARCHAR)";
    };
  }

  private static String structToSql(Schema schema, Struct struct) {
    StringBuilder builder = new StringBuilder("CAST(ROW(");
    boolean first = true;
    for (Field field : struct.schema().fields()) {
      if (!first) {
        builder.append(", ");
      }
      builder.append(toSqlLiteral(field.name(), field.schema(), struct.get(field)));
      first = false;
    }
    builder.append(") AS ROW(");
    first = true;
    for (Field field : struct.schema().fields()) {
      if (!first) {
        builder.append(", ");
      }
      builder.append('"').append(field.name()).append('"').append(' ');
      builder.append(trinoType(field.schema()));
      first = false;
    }
    builder.append("))");
    return builder.toString();
  }

  private static String cdcToSql(Struct cdc) {
    Struct key = (Struct) cdc.get("key");
    String keyJson = structToJson(key);
    String op = (String) cdc.get("op");
    Date ts = (Date) cdc.get("ts");
    Object offset = cdc.get("offset");

    return "CAST(ROW('"
        + escapeSql(keyJson)
        + "', '"
        + escapeSql(op)
        + "', TIMESTAMP '"
        + TIMESTAMP_FORMAT.format(ts)
        + "', "
        + (offset == null ? "NULL" : offset.toString())
        + ") AS ROW(\"key\" VARCHAR, op VARCHAR, ts TIMESTAMP(6), \"offset\" BIGINT))";
  }

  private static String structToJson(Struct struct) {
    StringBuilder builder = new StringBuilder("{");
    boolean first = true;
    for (Field field : struct.schema().fields()) {
      if (!first) {
        builder.append(',');
      }
      builder.append('"').append(field.name()).append('"').append(':');
      builder.append(jsonValue(field.schema(), struct.get(field)));
      first = false;
    }
    builder.append('}');
    return builder.toString();
  }

  private static String jsonValue(Schema schema, Object value) {
    if (value == null) {
      return "null";
    }
    if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      return '"' + ((BigDecimal) value).toPlainString() + '"';
    }
    if ("org.apache.kafka.connect.data.Timestamp".equals(schema.name())) {
      return String.valueOf(((Date) value).getTime());
    }

    return switch (schema.type()) {
      case INT8, INT16, INT32, INT64, FLOAT32, FLOAT64 -> value.toString();
      case BOOLEAN -> ((Boolean) value) ? "true" : "false";
      case STRING -> '"' + escapeJson(value.toString()) + '"';
      case BYTES -> '"' + Base64.getEncoder().encodeToString((byte[]) value) + '"';
      case STRUCT -> structToJson((Struct) value);
      default -> '"' + escapeJson(value.toString()) + '"';
    };
  }

  private static String trinoType(Schema schema) {
    if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      String precision = schema.parameters() == null ? "38" : schema.parameters().getOrDefault("connect.decimal.precision", "38");
      String scale = schema.parameters() == null ? "0" : schema.parameters().getOrDefault("scale", "0");
      return "DECIMAL(" + precision + ", " + scale + ")";
    }
    if ("org.apache.kafka.connect.data.Timestamp".equals(schema.name())) {
      return "TIMESTAMP(6)";
    }

    return switch (schema.type()) {
      case INT8, INT16, INT32 -> "INTEGER";
      case INT64 -> "BIGINT";
      case FLOAT32 -> "REAL";
      case FLOAT64 -> "DOUBLE";
      case BOOLEAN -> "BOOLEAN";
      case STRING -> "VARCHAR";
      case BYTES -> "VARBINARY";
      case STRUCT -> {
        StringBuilder builder = new StringBuilder("ROW(");
        boolean first = true;
        for (Field field : schema.fields()) {
          if (!first) {
            builder.append(", ");
          }
          builder.append('"').append(field.name()).append('"').append(' ').append(trinoType(field.schema()));
          first = false;
        }
        builder.append(')');
        yield builder.toString();
      }
      default -> "VARCHAR";
    };
  }

  private static String escapeSql(String value) {
    return value.replace("'", "''");
  }

  private static String escapeJson(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"");
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder builder = new StringBuilder();
    for (byte b : bytes) {
      builder.append(String.format("%02x", b));
    }
    return builder.toString();
  }

  private static SimpleDateFormat timestampFormatter() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format;
  }
}
