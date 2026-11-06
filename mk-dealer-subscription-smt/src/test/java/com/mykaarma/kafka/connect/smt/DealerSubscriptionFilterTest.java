package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DealerSubscriptionFilterTest {

  private HttpServer server;
  private final AtomicReference<String> snapshotBody = new AtomicReference<>();
  private final AtomicReference<String> versionBody = new AtomicReference<>("{\"version\": 1000}");
  private final AtomicInteger snapshotCalls = new AtomicInteger();
  private final AtomicReference<String> lastAuth = new AtomicReference<>();
  private DealerSubscriptionFilter<SinkRecord> smt;

  @Before
  public void setUp() throws IOException {
    snapshotBody.set(snapshot(1000,
        "{\"eventType\":\"pointintime\",\"dataType\":\"CUSTOMER\",\"dealerUuid\":\"d1\"},"
            + "{\"eventType\":\"updated\",\"dataType\":\"ORDER\",\"dealerUuid\":\"d2\"}"));
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/webhook/v1/subscriptions/snapshot", ex -> {
      snapshotCalls.incrementAndGet();
      lastAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
      reply(ex, 200, snapshotBody.get());
    });
    server.createContext("/webhook/v1/subscriptions/version", ex -> reply(ex, 200, versionBody.get()));
    server.start();
    smt = new DealerSubscriptionFilter<>();
  }

  @After
  public void tearDown() {
    smt.close();
    server.stop(0);
  }

  private static String snapshot(long version, String keys) {
    return "{\"version\":" + version + ",\"keys\":[" + keys + "]}";
  }

  private static void reply(com.sun.net.httpserver.HttpExchange ex, int status, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(status, bytes.length);
    ex.getResponseBody().write(bytes);
    ex.close();
  }

  private Map<String, Object> config(String... extra) {
    Map<String, Object> c = new HashMap<>();
    c.put("ops.api.base.url", "http://127.0.0.1:" + server.getAddress().getPort() + "/webhook/v1");
    c.put("ops.api.username", "smt");
    c.put("ops.api.password", "secret");
    for (int i = 0; i < extra.length; i += 2) {
      c.put(extra[i], extra[i + 1]);
    }
    return c;
  }

  private static SinkRecord record(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0);
  }

  private static Map<String, Object> event(String dealer, String dataType, String eventType) {
    Map<String, Object> m = new HashMap<>();
    m.put("dealerUuid", dealer);
    m.put("dataType", dataType);
    if (eventType != null) {
      m.put("eventType", eventType);
    }
    return m;
  }

  @Test
  public void subscribedKeyPasses() {
    smt.configure(config());
    SinkRecord r = record(event("d1", "CUSTOMER", "pointintime"));
    assertSame(r, smt.apply(r));
  }

  @Test
  public void unsubscribedDealerIsDropped() {
    smt.configure(config());
    assertNull(smt.apply(record(event("other", "CUSTOMER", "pointintime"))));
  }

  @Test
  public void missingEventTypeMeansPointintime() {
    smt.configure(config());
    assertNotNull(smt.apply(record(event("d1", "CUSTOMER", null))));
    assertNotNull(smt.apply(record(event("d1", "CUSTOMER", ""))));
    assertNull(smt.apply(record(event("d2", "ORDER", null))));
  }

  @Test
  public void missingDealerOrDataTypeIsDropped() {
    smt.configure(config());
    Map<String, Object> noDealer = event("d1", "CUSTOMER", null);
    noDealer.remove("dealerUuid");
    assertNull(smt.apply(record(noDealer)));
  }

  @Test
  public void structValuesAreRead() {
    smt.configure(config());
    Schema schema = SchemaBuilder.struct().field("dealerUuid", Schema.STRING_SCHEMA)
        .field("dataType", Schema.STRING_SCHEMA).build();
    Struct s = new Struct(schema).put("dealerUuid", "d1").put("dataType", "CUSTOMER");
    assertNotNull(smt.apply(record(s)));
  }

  @Test
  public void nestedEventTypePath() {
    smt.configure(config("field.event.type", "detail.eventType"));
    Map<String, Object> m = event("d2", "ORDER", null);
    Map<String, Object> detail = new HashMap<>();
    detail.put("eventType", "updated");
    m.put("detail", detail);
    assertNotNull(smt.apply(record(m)));
  }

  @Test
  public void nullValuePassesThrough() {
    smt.configure(config());
    SinkRecord r = record(null);
    assertSame(r, smt.apply(r));
  }

  @Test
  public void shadowModeNeverDrops() {
    smt.configure(config("mode", "shadow"));
    SinkRecord r = record(event("other", "CUSTOMER", "pointintime"));
    assertSame(r, smt.apply(r));
  }

  @Test
  public void sendsBasicAuth() {
    smt.configure(config());
    assertTrue(lastAuth.get().startsWith("Basic "));
  }

  @Test
  public void versionChangeReloadsSnapshot() {
    smt.configure(config());
    assertNull(smt.apply(record(event("d3", "CUSTOMER", "pointintime"))));
    snapshotBody.set(snapshot(2000,
        "{\"eventType\":\"pointintime\",\"dataType\":\"CUSTOMER\",\"dealerUuid\":\"d3\"}"));
    versionBody.set("{\"version\": 2000}");
    smt.poll();
    assertNotNull(smt.apply(record(event("d3", "CUSTOMER", "pointintime"))));
    assertNull(smt.apply(record(event("d1", "CUSTOMER", "pointintime"))));
  }

  @Test
  public void unchangedOldVersionDoesNotReload() {
    smt.configure(config());
    int before = snapshotCalls.get();
    smt.poll();
    assertEquals(before, snapshotCalls.get());
  }

  @Test
  public void pollFailureKeepsCurrentSnapshot() {
    smt.configure(config());
    versionBody.set("not json");
    smt.poll();
    assertNotNull(smt.apply(record(event("d1", "CUSTOMER", "pointintime"))));
  }

  @Test(expected = ConnectException.class)
  public void failedInitialLoadFailsConfigure() {
    server.stop(0);
    smt.configure(config());
  }
}
