package com.mykaarma.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Serializes some or all of a record's Kafka headers into a JSON array of
 * {"key": ..., "value": ...} objects and adds it as a field on the record
 * value - without needing to know the header keys ahead of time.
 *
 * <p>Works whether the value is a schema'd Struct or a schemaless Map. Records
 * with no headers, or a null value, pass through unchanged.
 */
public class HeaderToValueJson<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELD_NAME_CONFIG = "field.name";
  public static final String HEADERS_CONFIG = "headers";
  public static final String REMOVE_HEADERS_CONFIG = "headers.remove";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(
          FIELD_NAME_CONFIG,
          ConfigDef.Type.STRING,
          "_headers_json",
          ConfigDef.Importance.MEDIUM,
          "Name of the value field to hold the JSON-encoded headers.")
      .define(
          HEADERS_CONFIG,
          ConfigDef.Type.LIST,
          Collections.emptyList(),
          ConfigDef.Importance.MEDIUM,
          "Optional allow-list of header keys to include. Leave empty (default) to include "
              + "every header on the record, whatever its key is.")
      .define(
          REMOVE_HEADERS_CONFIG,
          ConfigDef.Type.BOOLEAN,
          false,
          ConfigDef.Importance.LOW,
          "If true, drops the headers from the record after copying them into the value "
              + "(equivalent to HeaderToValue's operation=move). Defaults to false (copy).");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String fieldName;
  private Set<String> allowedHeaderKeys;
  private boolean removeHeaders;
  private final Map<Schema, Schema> schemaUpdateCache = new ConcurrentHashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    fieldName = config.getString(FIELD_NAME_CONFIG);
    List<String> headers = config.getList(HEADERS_CONFIG);
    allowedHeaderKeys = headers.isEmpty() ? null : new HashSet<>(headers);
    removeHeaders = config.getBoolean(REMOVE_HEADERS_CONFIG);
  }

  @Override
  public R apply(R record) {
    if (record.headers() == null || record.headers().isEmpty() || record.value() == null) {
      return record;
    }

    String json = headersToJson(record.headers());
    Object value = record.value();

    Object updatedValue;
    Schema updatedSchema;

    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      updatedSchema = schemaUpdateCache.computeIfAbsent(struct.schema(), this::withJsonField);
      Struct updatedStruct = new Struct(updatedSchema);
      for (Field f : struct.schema().fields()) {
        updatedStruct.put(f.name(), struct.get(f));
      }
      updatedStruct.put(fieldName, json);
      updatedValue = updatedStruct;
    } else if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> original = (Map<String, Object>) value;
      Map<String, Object> updatedMap = new LinkedHashMap<>(original);
      updatedMap.put(fieldName, json);
      updatedValue = updatedMap;
      updatedSchema = null;
    } else {
      throw new DataException(
          "HeaderToValueJson requires the record value to be a Struct or a Map, but got: "
              + value.getClass().getName());
    }

    Headers outputHeaders = removeHeaders ? new ConnectHeaders() : record.headers();

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp(),
        outputHeaders);
  }

  private Schema withJsonField(Schema original) {
    SchemaBuilder builder = SchemaBuilder.struct();
    if (original.name() != null) {
      builder.name(original.name());
    }
    for (Field f : original.fields()) {
      builder.field(f.name(), f.schema());
    }
    builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
    return builder.build();
  }

  private String headersToJson(Headers headers) {
    List<Map<String, String>> entries = new ArrayList<>();
    for (Header h : headers) {
      if (allowedHeaderKeys != null && !allowedHeaderKeys.contains(h.key())) {
        continue;
      }
      Map<String, String> entry = new LinkedHashMap<>();
      entry.put("key", h.key());
      entry.put("value", Values.convertToString(h.schema(), h.value()));
      entries.add(entry);
    }
    try {
      return MAPPER.writeValueAsString(entries);
    } catch (Exception e) {
      throw new DataException("Failed to serialize record headers to JSON", e);
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache.clear();
  }
}
