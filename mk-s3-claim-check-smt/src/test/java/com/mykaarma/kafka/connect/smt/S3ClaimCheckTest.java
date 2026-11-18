package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

public class S3ClaimCheckTest {

  /** In-memory store that can be told to fail. Keys follow the ops API: prefix/dealer/eventId. */
  static class FakeStore implements BodyStore {
    final Map<String, byte[]> objects = new LinkedHashMap<>();
    boolean fail;
    boolean reject;
    boolean probed;
    boolean rejectProbe;

    @Override
    public void probe() throws IOException {
      if (rejectProbe) {
        throw new RejectedException("POST /claim-check returned HTTP 404");
      }
      if (fail) {
        throw new IOException("denied");
      }
      probed = true;
    }

    @Override
    public Location put(String eventId, String dealerUuid, byte[] body) throws IOException {
      if (reject) {
        throw new RejectedException("HTTP 400");
      }
      if (fail) {
        throw new IOException("unavailable");
      }
      String key = "oversize/kafka-connect/2026-11-17/" + dealerUuid + "/" + eventId + ".json";
      objects.put(key, body);
      return new Location("claims", key);
    }
  }

  private FakeStore store;
  private S3ClaimCheck<SinkRecord> smt;

  @Before
  public void setUp() {
    store = new FakeStore();
    smt = new S3ClaimCheck<SinkRecord>() {
      @Override
      BodyStore createStore(AbstractConfig config) {
        return store;
      }
    };
  }

  private Map<String, Object> config(String... extra) {
    Map<String, Object> c = new HashMap<>();
    c.put("ops.api.base.url", "https://ops.example/webhook/v1");
    c.put("ops.api.username", "user");
    c.put("ops.api.password", "secret");
    c.put("large.event.bytes", "200");
    for (int i = 0; i < extra.length; i += 2) {
      c.put(extra[i], extra[i + 1]);
    }
    return c;
  }

  private static SinkRecord record(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0);
  }

  private static Map<String, Object> event(String eventId, int payloadChars) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("dealerUuid", "d1");
    m.put("dataType", "CUSTOMER");
    m.put("eventId", eventId);
    m.put("payload", "x".repeat(payloadChars));
    return m;
  }

  @Test
  public void smallRecordPassesUntouched() {
    smt.configure(config());
    SinkRecord r = record(event("e1", 10));
    assertSame(r, smt.apply(r));
    assertTrue(store.probed);
    assertEquals(0, store.objects.size());
  }

  @Test
  public void largeRecordBecomesPointer() {
    smt.configure(config());
    SinkRecord out = smt.apply(record(event("e1", 500)));
    assertNotSame(null, out);
    @SuppressWarnings("unchecked")
    Map<String, Object> pointer = (Map<String, Object>) out.value();
    assertEquals("d1", pointer.get("dealerUuid"));
    assertEquals("CUSTOMER", pointer.get("dataType"));
    assertEquals("e1", pointer.get("eventId"));
    assertTrue(!pointer.containsKey("payload"));
    @SuppressWarnings("unchecked")
    Map<String, Object> claim = (Map<String, Object>) pointer.get("claimCheck");
    assertEquals("claims", claim.get("bucket"));
    String key = "oversize/kafka-connect/2026-11-17/d1/e1.json";
    assertEquals(key, claim.get("key"));
    assertTrue(new String(store.objects.get(key)).contains("xxxxxxxx"));
  }

  @Test
  public void retryOverwritesSameKey() {
    smt.configure(config());
    smt.apply(record(event("e1", 500)));
    smt.apply(record(event("e1", 500)));
    assertEquals(1, store.objects.size());
  }

  @Test
  public void missingEventIdUsesContentHash() {
    smt.configure(config());
    Map<String, Object> e = event("e1", 500);
    e.remove("eventId");
    SinkRecord out = smt.apply(record(e));
    @SuppressWarnings("unchecked")
    Map<String, Object> claim = (Map<String, Object>) ((Map<String, Object>) out.value())
        .get("claimCheck");
    assertTrue(claim.get("key").toString().matches("oversize/kafka-connect/[^/]+/d1/[0-9a-f]{64}\\.json"));
  }

  @Test
  public void structValueIsClaimChecked() {
    smt.configure(config());
    Schema schema = SchemaBuilder.struct().field("dealerUuid", Schema.STRING_SCHEMA)
        .field("eventId", Schema.STRING_SCHEMA).field("payload", Schema.STRING_SCHEMA).build();
    Struct s = new Struct(schema).put("dealerUuid", "d1").put("eventId", "e9")
        .put("payload", "y".repeat(500));
    SinkRecord out = smt.apply(record(s));
    assertNull(out.valueSchema());
    assertTrue(store.objects.containsKey("oversize/kafka-connect/2026-11-17/d1/e9.json"));
  }

  @Test
  public void nullValuePassesThrough() {
    smt.configure(config());
    SinkRecord r = record(null);
    assertSame(r, smt.apply(r));
  }

  @Test(expected = RetriableException.class)
  public void uploadFailureIsRetriable() {
    smt.configure(config());
    store.fail = true;
    smt.apply(record(event("e1", 500)));
  }

  @Test
  public void rejectedUploadIsNotRetriedAndCarriesNoCustomCause() {
    smt.configure(config());
    store.reject = true;
    try {
      smt.apply(record(event("e1", 500)));
      fail();
    } catch (DataException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains("HTTP 400"));
    }
  }

  @Test
  public void rejectedProbeFailsAtStartupWithoutACustomCause() {
    store.rejectProbe = true;
    try {
      smt.configure(config());
      fail();
    } catch (ConnectException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains("HTTP 404"));
    }
  }

  @Test
  public void unsafeIdsAreMadeSafeForTheKey() {
    smt.configure(config());
    Map<String, Object> e = event("a/b c", 500);
    e.put("dealerUuid", "../x");
    smt.apply(record(e));
    assertTrue(store.objects.containsKey("oversize/kafka-connect/2026-11-17/.._x/a_b_c.json"));
  }

  @Test
  public void missingDealerUsesUnknown() {
    smt.configure(config());
    Map<String, Object> e = event("e1", 500);
    e.remove("dealerUuid");
    smt.apply(record(e));
    assertTrue(store.objects.containsKey("oversize/kafka-connect/2026-11-17/unknown/e1.json"));
  }

  @Test(expected = ConnectException.class)
  public void unreachableRouteFailsAtStartup() {
    store.fail = true;
    smt.configure(config());
  }
}
