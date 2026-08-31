package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeyToValueTest {

  private KeyToValue<SinkRecord> smt;

  @Before
  public void setUp() {
    smt = new KeyToValue<>();
    smt.configure(Collections.emptyMap());
  }

  @After
  public void tearDown() {
    smt.close();
  }

  @Test
  public void embedsSchemaBackedKeyOnStructValue() {
    Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .build();
    Struct key = new Struct(keySchema).put("id", 5001);

    Schema valueSchema = SchemaBuilder.struct()
        .field("table", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(valueSchema).put("table", "customers");

    SinkRecord record = new SinkRecord(
        "dbserver1.inventory.customers", 0, keySchema, key, valueSchema, value, 0L);

    SinkRecord result = smt.apply(record);

    Struct updated = (Struct) result.value();
    assertEquals("customers", updated.getString("table"));
    Struct embeddedKey = (Struct) updated.get("_key");
    assertEquals(5001, (int) embeddedKey.getInt32("id"));
  }

  @Test
  public void infersSchemaForSchemalessPrimitiveKey() {
    Schema valueSchema = SchemaBuilder.struct()
        .field("table", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(valueSchema).put("table", "customers");

    SinkRecord record = new SinkRecord(
        "topic", 0, null, "row-5001", valueSchema, value, 0L);

    SinkRecord result = smt.apply(record);

    Struct updated = (Struct) result.value();
    assertEquals("row-5001", updated.getString("_key"));
  }

  @Test
  public void addsKeyToSchemalessMapValue() {
    Map<String, Object> value = new HashMap<>();
    value.put("table", "customers");

    SinkRecord record = new SinkRecord(
        "topic", 0, null, "row-5001", null, value, 0L);

    SinkRecord result = smt.apply(record);

    @SuppressWarnings("unchecked")
    Map<String, Object> updated = (Map<String, Object>) result.value();
    assertEquals("row-5001", updated.get("_key"));
  }

  @Test
  public void passesThroughTombstones() {
    Schema keySchema = Schema.STRING_SCHEMA;
    SinkRecord record = new SinkRecord(
        "topic", 0, keySchema, "row-5001", null, null, 0L);

    SinkRecord result = smt.apply(record);

    assertNull(result.value());
  }
}
