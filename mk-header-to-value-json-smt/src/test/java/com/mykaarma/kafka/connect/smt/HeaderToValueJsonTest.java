package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HeaderToValueJsonTest {

  private HeaderToValueJson<SinkRecord> smt;

  @Before
  public void setUp() {
    smt = new HeaderToValueJson<>();
  }

  @After
  public void tearDown() {
    smt.close();
  }

  @Test
  public void addsAllHeadersAsJsonOnStructValue() {
    smt.configure(Collections.emptyMap());

    Schema valueSchema = SchemaBuilder.struct()
        .field("table", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(valueSchema).put("table", "customers");

    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("op", "c");
    headers.addString("row-id", "5001");

    SinkRecord record = new SinkRecord(
        "seed_parquet_topic", 0, null, null, valueSchema, value, 0L,
        null, null, headers);

    SinkRecord result = smt.apply(record);

    Struct updated = (Struct) result.value();
    assertEquals("customers", updated.getString("table"));
    String json = updated.getString("_headers_json");
    assertTrue(json.contains("\"key\":\"op\""));
    assertTrue(json.contains("\"value\":\"c\""));
    assertTrue(json.contains("row-id"));
  }

  @Test
  public void addsAllowListedHeadersOnlyAndRemovesThem() {
    Map<String, Object> config = new HashMap<>();
    config.put(HeaderToValueJson.HEADERS_CONFIG, Collections.singletonList("op"));
    config.put(HeaderToValueJson.REMOVE_HEADERS_CONFIG, true);
    smt.configure(config);

    ConnectHeaders headers = new ConnectHeaders();
    headers.addString("op", "c");
    headers.addString("source", "seed_parquet_topic.py");

    Map<String, Object> value = new HashMap<>();
    value.put("table", "customers");

    SinkRecord record = new SinkRecord(
        "seed_parquet_topic", 0, null, null, null, value, 0L,
        null, null, headers);

    SinkRecord result = smt.apply(record);

    @SuppressWarnings("unchecked")
    Map<String, Object> updated = (Map<String, Object>) result.value();
    String json = (String) updated.get("_headers_json");
    assertTrue(json.contains("\"op\""));
    assertFalse(json.contains("source"));
    assertTrue(result.headers().isEmpty());
  }

  @Test
  public void passesThroughRecordsWithoutHeaders() {
    smt.configure(Collections.emptyMap());

    Map<String, Object> value = new HashMap<>();
    value.put("table", "customers");

    SinkRecord record = new SinkRecord(
        "seed_parquet_topic", 0, null, null, null, value, 0L);

    SinkRecord result = smt.apply(record);

    assertEquals(value, result.value());
  }
}
