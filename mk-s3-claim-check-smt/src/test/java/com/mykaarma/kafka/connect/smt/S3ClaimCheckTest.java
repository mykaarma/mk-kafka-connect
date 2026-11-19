package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    IOException failure;

    @Override
    public void probe() throws IOException {
      if (failure != null) {
        throw failure;
      }
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
      if (failure != null) {
        throw failure;
      }
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

  /** The key the SMT builds: the dealer and the event id each carry a hash of the original. A missing dealer is the bare fallback. */
  private static String keyFor(String dealer, String eventId) {
    String dealerSegment = dealer == null ? "unknown" : S3ClaimCheck.safe(dealer, "unknown");
    return "oversize/kafka-connect/2026-11-17/" + dealerSegment + "/" + S3ClaimCheck.keyName(eventId, new byte[0]) + ".json";
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
    String key = keyFor("d1", "e1");
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
    assertTrue(claim.get("key").toString().matches("oversize/kafka-connect/[^/]+/d1-[0-9a-f]{64}/[0-9a-f]{64}\\.json"));
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
    assertTrue(store.objects.containsKey(keyFor("d1", "e9")));
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
    assertEquals(1, store.objects.size());
    String key = store.objects.keySet().iterator().next();
    // Both ids changed, so each carries a hash of the original: 12 hex characters.
    assertTrue(key, key.matches("oversize/kafka-connect/2026-11-17/\\.\\._x-[0-9a-f]{64}/a_b_c-[0-9a-f]{64}\\.json"));
  }

  @Test
  public void idsThatSanitizeToTheSameStringStayApart() {
    smt.configure(config());
    smt.apply(record(event("a/b c", 500)));
    smt.apply(record(event("a_b_c", 500)));
    // A retry overwrites the object at its key, so two ids must not share one.
    assertEquals(2, store.objects.size());
    assertTrue(store.objects.containsKey(keyFor("d1", "a/b c")));
    assertTrue(store.objects.containsKey(keyFor("d1", "a_b_c")));
  }

  @Test
  public void anIdThatLooksLikeAGeneratedKeyCannotTakeItsPlace() {
    smt.configure(config());
    String generated = S3ClaimCheck.keyName("a/b c", new byte[0]); // a_b_c-<12 hex>
    smt.apply(record(event("a/b c", 500)));
    smt.apply(record(event(generated, 500)));
    // An unchanged id used to pass through as itself, which here equals the first event's key.
    assertEquals(2, store.objects.size());
  }

  @Test
  public void aRetryOfAnUnsafeIdStillLandsOnTheSameKey() {
    smt.configure(config());
    smt.apply(record(event("a/b c", 500)));
    smt.apply(record(event("a/b c", 500)));
    assertEquals(1, store.objects.size());
  }

  @Test
  public void longIdsAreTruncatedWithoutCollidingOrExceedingTheLimit() {
    smt.configure(config());
    String prefix = "x".repeat(200);
    smt.apply(record(event(prefix + "-one", 500)));
    smt.apply(record(event(prefix + "-two", 500)));
    assertEquals(2, store.objects.size());
    for (String key : store.objects.keySet()) {
      String id = key.substring(key.lastIndexOf('/') + 1, key.length() - ".json".length());
      assertTrue(id, id.length() <= 128);
    }
  }

  @Test
  public void everyValueCarriesAHashAndEmptyOnesFallBack() {
    assertTrue(S3ClaimCheck.safe("603588ac-c548-49bf-9248-14b899aaeec9", "event")
        .matches("603588ac-c548-49bf-9248-14b899aaeec9-[0-9a-f]{64}"));
    assertEquals("unknown", S3ClaimCheck.safe(null, "unknown"));
    assertEquals("unknown", S3ClaimCheck.safe("", "unknown"));
    // The fallback is bare, so a value that is literally "unknown" cannot take its place.
    assertTrue(S3ClaimCheck.safe("unknown", "unknown").matches("unknown-[0-9a-f]{64}"));
  }

  /**
   * The key must be an injective function of the original (dealer, event id) pair. Cross values that
   * sanitize to the same string, values that look like generated segments, the fallbacks, a value that
   * needs truncating at either end, and plain ids: every pair gets its own object.
   */
  @Test
  public void everyDistinctDealerAndEventIdPairGetsItsOwnKey() {
    smt.configure(config());
    String generatedLooking = S3ClaimCheck.safe("a/b c", "x");
    java.util.List<String> values = java.util.Arrays.asList(
        "a/b c", "a_b_c", "a b c", generatedLooking, "unknown", "event", "e1", "e1 ", "e1-",
        "x".repeat(200) + "-one", "x".repeat(200) + "-two", "x".repeat(128), "x".repeat(129),
        "603588ac-c548-49bf-9248-14b899aaeec9",
        // Unpaired surrogates are distinct strings, though getBytes(UTF_8) turns each into "?".
        "\uD800", "\uD801", "a\uD800", "a\uD801");
    int pairs = 0;
    for (String dealer : values) {
      for (String id : values) {
        Map<String, Object> e = event(id, 500);
        e.put("dealerUuid", dealer);
        smt.apply(record(e));
        pairs++;
      }
    }
    assertEquals(pairs, store.objects.size());
    for (String key : store.objects.keySet()) {
      for (String segment : key.substring("oversize/kafka-connect/2026-11-17/".length()).split("/")) {
        assertTrue(segment, segment.length() <= 128 + ".json".length());
      }
    }
  }

  /** Found by review: both ids start with the digest 69c16c8db1cf, and cutting the readable part makes the prefixes equal. */
  @Test
  public void idsWhoseDigestsShareAPrefixStayApartAfterTruncation() {
    String a = "x".repeat(115) + "3993167";
    String b = "x".repeat(115) + "16104942";
    String segmentA = S3ClaimCheck.safe(a, "x");
    String segmentB = S3ClaimCheck.safe(b, "x");
    assertFalse(segmentA.equals(segmentB));
    assertEquals(128, segmentA.length());
    assertEquals(128, segmentB.length());
  }

  @Test
  public void idsThatDifferOnlyByAnUnpairedSurrogateGetDifferentSegments() {
    assertFalse(S3ClaimCheck.safe("\uD800", "x").equals(S3ClaimCheck.safe("\uD801", "x")));
    assertTrue(S3ClaimCheck.safe("\uD800", "x").matches("_-[0-9a-f]{64}"));
  }

  @Test
  public void dealersThatSanitizeToTheSameStringStayApart() {
    smt.configure(config());
    Map<String, Object> a = event("e1", 500);
    a.put("dealerUuid", "a/b c");
    Map<String, Object> b = event("e1", 500);
    b.put("dealerUuid", S3ClaimCheck.safe("a/b c", "unknown")); // looks like the generated segment
    smt.apply(record(a));
    smt.apply(record(b));
    assertEquals(2, store.objects.size());
  }

  @Test
  public void missingDealerUsesUnknown() {
    smt.configure(config());
    Map<String, Object> e = event("e1", 500);
    e.remove("dealerUuid");
    smt.apply(record(e));
    assertTrue(store.objects.containsKey(keyFor(null, "e1")));
  }

  /** A shaded Jackson exception: the worker cannot rebuild it, so it must not leave the SMT. */
  private static IOException withJacksonCause() {
    return new IOException("bad body from the route",
        new com.fasterxml.jackson.core.JsonParseException(null, "Unexpected character"));
  }

  @Test
  public void startupFailureDropsAJacksonCauseAndKeepsTheMessage() {
    store.failure = withJacksonCause();
    try {
      smt.configure(config());
      fail();
    } catch (ConnectException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains("bad body from the route"));
    }
  }

  @Test
  public void retriableUploadFailureDropsAJacksonCauseAndKeepsTheMessage() {
    smt.configure(config());
    store.failure = withJacksonCause();
    try {
      smt.apply(record(event("e1", 500)));
      fail();
    } catch (RetriableException e) {
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains("bad body from the route"));
    }
  }

  @Test
  public void aJdkCauseIsKept() {
    smt.configure(config());
    IOException timeout = new IOException("timed out", new java.net.SocketTimeoutException("read"));
    store.failure = timeout;
    try {
      smt.apply(record(event("e1", 500)));
      fail();
    } catch (RetriableException e) {
      assertSame(timeout, e.getCause());
    }
  }

  @Test
  public void serializationFailureDropsTheJacksonCause() {
    smt.configure(config());
    Map<String, Object> e = event("e1", 500);
    e.put("unserializable", new Object());
    try {
      smt.apply(record(e));
      fail();
    } catch (DataException ex) {
      assertNull(ex.getCause());
      assertTrue(ex.getMessage().startsWith("Could not serialize the record value to JSON"));
    }
  }

  @Test
  public void safeCauseKeepsOnlyJdkAndKafkaChains() {
    IOException plain = new IOException("x", new IllegalStateException("y"));
    assertSame(plain, S3ClaimCheck.safeCause(plain));
    assertSame(plain.getCause(), S3ClaimCheck.safeCause(plain.getCause()));
    assertNull(S3ClaimCheck.safeCause(new BodyStore.RejectedException("HTTP 400")));
    assertNull(S3ClaimCheck.safeCause(withJacksonCause()));
    // One foreign class anywhere in the chain is enough to drop it.
    assertNull(S3ClaimCheck.safeCause(new IOException("outer", withJacksonCause())));
    assertSame(null, S3ClaimCheck.safeCause(null));
  }

  @Test(expected = ConnectException.class)
  public void unreachableRouteFailsAtStartup() {
    store.fail = true;
    smt.configure(config());
  }
}
