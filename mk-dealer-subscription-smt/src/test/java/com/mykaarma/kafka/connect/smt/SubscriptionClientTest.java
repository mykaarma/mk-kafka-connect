package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Runs the client against a local server that plays the ops API. */
public class SubscriptionClientTest {

  private HttpServer server;
  private String base;
  private volatile String body = "{\"version\":7,\"keys\":[]}";

  @Before
  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort() + "/webhook/v1";
    server.createContext("/webhook/v1/subscriptions", ex -> {
      byte[] out = body.getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(200, out.length);
      ex.getResponseBody().write(out);
      ex.close();
    });
    server.start();
  }

  @After
  public void stop() {
    server.stop(0);
  }

  private SubscriptionClient client() {
    return new SubscriptionClient(base, "user", "pass", 1000, 2000);
  }

  @Test
  public void validSnapshotLoads() throws IOException {
    assertEquals(7L, client().snapshot().version);
  }

  @Test
  public void malformedJsonIsAPlainIoExceptionWithNoCause() {
    body = "{\"version\": not json";
    try {
      client().snapshot();
      fail();
    } catch (IOException e) {
      // Exactly IOException: a shaded Jackson exception would not survive the function runtime.
      assertEquals(IOException.class, e.getClass());
      assertNull(e.getCause());
      assertTrue(e.getMessage().contains("malformed JSON"));
    }
  }

  @Test
  public void malformedSnapshotFailsWithTheReasonAndNoJacksonCause() {
    body = "{\"version\": not json";
    try {
      client().snapshot();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("malformed JSON"));
      for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
        assertTrue("foreign class in the cause chain: " + c.getClass(),
            c.getClass().getName().startsWith("java."));
      }
    }
  }
}
