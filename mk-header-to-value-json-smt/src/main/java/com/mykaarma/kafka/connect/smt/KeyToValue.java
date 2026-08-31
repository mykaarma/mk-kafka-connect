package com.mykaarma.kafka.connect.smt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Copies the Kafka record key onto the record value as a nested field, so
 * downstream storage sees the key alongside the value instead of (or as well
 * as) in a separate keys file/topic.
 *
 * <p>Unlike headers, a record has exactly one key with one (mostly stable)
 * schema, so it is embedded natively rather than flattened to JSON: if the
 * key has a Connect schema it is nested as-is (preserving Struct/primitive
 * typing); if the key is schemaless, a best-effort primitive schema is
 * inferred from its runtime Java type. Records with a null key or null value
 * (tombstones) pass through unchanged.
 */
public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELD_NAME_CONFIG = "field.name";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          FIELD_NAME_CONFIG,
          ConfigDef.Type.STRING,
          "_key",
          ConfigDef.Importance.MEDIUM,
          "Name of the value field to hold the record key.");

  private String fieldName;
  private final Map<SchemaPair, Schema> schemaUpdateCache = new ConcurrentHashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    fieldName = config.getString(FIELD_NAME_CONFIG);
  }

  @Override
  public R apply(R record) {
    if (record.key() == null || record.value() == null) {
      return record;
    }

    Schema keySchema = record.keySchema() != null
        ? record.keySchema()
        : inferSchema(record.key());

    Object value = record.value();
    Object updatedValue;
    Schema updatedSchema;

    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      SchemaPair cacheKey = new SchemaPair(struct.schema(), keySchema);
      updatedSchema = schemaUpdateCache.computeIfAbsent(cacheKey, this::withKeyField);
      Struct updatedStruct = new Struct(updatedSchema);
      for (Field f : struct.schema().fields()) {
        updatedStruct.put(f.name(), struct.get(f));
      }
      updatedStruct.put(fieldName, record.key());
      updatedValue = updatedStruct;
    } else if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> original = (Map<String, Object>) value;
      Map<String, Object> updatedMap = new java.util.LinkedHashMap<>(original);
      updatedMap.put(fieldName, record.key());
      updatedValue = updatedMap;
      updatedSchema = null;
    } else {
      throw new DataException(
          "KeyToValue requires the record value to be a Struct or a Map, but got: "
              + value.getClass().getName());
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp(),
        record.headers());
  }

  private Schema withKeyField(SchemaPair pair) {
    SchemaBuilder builder = SchemaBuilder.struct();
    if (pair.valueSchema.name() != null) {
      builder.name(pair.valueSchema.name());
    }
    for (Field f : pair.valueSchema.fields()) {
      builder.field(f.name(), f.schema());
    }
    builder.field(fieldName, pair.keySchema);
    return builder.build();
  }

  private static Schema inferSchema(Object key) {
    if (key instanceof String) {
      return Schema.STRING_SCHEMA;
    } else if (key instanceof Integer) {
      return Schema.INT32_SCHEMA;
    } else if (key instanceof Long) {
      return Schema.INT64_SCHEMA;
    } else if (key instanceof Double) {
      return Schema.FLOAT64_SCHEMA;
    } else if (key instanceof Float) {
      return Schema.FLOAT32_SCHEMA;
    } else if (key instanceof Boolean) {
      return Schema.BOOLEAN_SCHEMA;
    } else if (key instanceof byte[]) {
      return Schema.BYTES_SCHEMA;
    }
    // Fall back to a string representation for anything else we don't
    // recognize, rather than failing the record outright.
    return Schema.STRING_SCHEMA;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache.clear();
  }

  private static final class SchemaPair {
    private final Schema valueSchema;
    private final Schema keySchema;

    private SchemaPair(Schema valueSchema, Schema keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SchemaPair)) {
        return false;
      }
      SchemaPair other = (SchemaPair) o;
      return valueSchema.equals(other.valueSchema) && keySchema.equals(other.keySchema);
    }

    @Override
    public int hashCode() {
      return 31 * valueSchema.hashCode() + keySchema.hashCode();
    }
  }
}
