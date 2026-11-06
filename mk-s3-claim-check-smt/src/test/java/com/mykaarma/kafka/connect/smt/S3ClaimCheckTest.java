package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

public class S3ClaimCheckTest {

  /** In-memory store that can be told to fail. */
  static class FakeStore implements BodyStore {
    final Map<String, byte[]> objects = new LinkedHashMap<>();
    boolean fail;

    @Override
    public void put(String key, byte[] body) throws IOException {
      if (fail) {
        throw new IOException("denied");
      }
      objects.put(key, body);
    }

    @Override
    public String bucket() {
      return "claims";
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
    c.put("large.event.s3.bucket", "claims");
    c.put("large.event.s3.region", "us-east-1");
    c.put("large.event.aws.access.key.id", "AKIA");
    c.put("large.event.aws.secret.access.key", "secret");
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
    assertEquals(1, store.objects.size()); // only the probe
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
    assertEquals("claim-check/e1.json", claim.get("key"));
    assertTrue(new String(store.objects.get("claim-check/e1.json")).contains("xxxxxxxx"));
  }

  @Test
  public void retryOverwritesSameKey() {
    smt.configure(config());
    smt.apply(record(event("e1", 500)));
    smt.apply(record(event("e1", 500)));
    assertEquals(2, store.objects.size()); // probe + one object
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
    assertTrue(claim.get("key").toString().matches("claim-check/[0-9a-f]{64}\\.json"));
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
    assertTrue(store.objects.containsKey("claim-check/e9.json"));
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

  @Test(expected = ConnectException.class)
  public void badCredentialFailsAtStartup() {
    store.fail = true;
    smt.configure(config());
  }
}
