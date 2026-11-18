package com.mykaarma.kafka.connect.smt;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Runs the store against a local server that plays the ops API and S3. */
public class ApiBodyStoreTest {

  private HttpServer server;
  private String base;
  private volatile int routeStatus = 200;
  private volatile int uploadStatus = 200;
  private volatile String routeBody;
  private volatile String routeAuth;
  private volatile String routeRequest;
  private volatile byte[] uploaded;
  private volatile String uploadLength;
  private volatile String uploadAuth;

  @Before
  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    routeBody = "{\"putUrl\":\"" + base + "/s3/the-key?X-Amz-Signature=abc\","
        + "\"bucket\":\"claims\",\"key\":\"oversize/kafka-connect/2026-11-17/d1/e1.json\"}";
    server.createContext("/webhook/v1/claim-check", ex -> {
      routeAuth = ex.getRequestHeaders().getFirst("Authorization");
      routeRequest = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      byte[] out = (routeStatus == 200 ? routeBody : "{}").getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(routeStatus, out.length);
      ex.getResponseBody().write(out);
      ex.close();
    });
    server.createContext("/s3", ex -> {
      uploaded = ex.getRequestBody().readAllBytes();
      uploadLength = ex.getRequestHeaders().getFirst("Content-Length");
      uploadAuth = ex.getRequestHeaders().getFirst("Authorization");
      ex.sendResponseHeaders(uploadStatus, -1);
      ex.close();
    });
    server.start();
  }

  @After
  public void stop() {
    server.stop(0);
  }

  private ApiBodyStore store() {
    return new ApiBodyStore(base + "/webhook/v1/", "user", "pass", 1000, 2000, 2000);
  }

  @Test
  public void uploadsTheBodyToTheIssuedUrl() throws IOException {
    byte[] body = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);

    BodyStore.Location where = store().put("e1", "d1", body);

    assertEquals("claims", where.bucket);
    assertEquals("oversize/kafka-connect/2026-11-17/d1/e1.json", where.key);
    assertArrayEquals(body, uploaded);
    assertEquals(String.valueOf(body.length), uploadLength);
    assertEquals("{\"eventId\":\"e1\",\"dealerUuid\":\"d1\",\"bytes\":7}", routeRequest);
  }

  @Test
  public void sendsBasicAuthToTheRouteOnly() throws IOException {
    store().put("e1", "d1", new byte[] {1});

    assertEquals("Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes()), routeAuth);
    // The issued URL is the credential for the upload, so the ops API login must not go with it.
    assertNull(uploadAuth);
  }

  @Test
  public void probeAsksForAUrlAndUploadsNothing() throws IOException {
    store().probe();

    assertTrue(routeRequest.contains("\"bytes\":1"));
    assertNull(uploaded);
  }

  @Test
  public void clientErrorFromTheRouteIsRejected() {
    routeStatus = 401;
    try {
      store().put("e1", "d1", new byte[] {1});
      fail();
    } catch (BodyStore.RejectedException expected) {
      assertTrue(expected.getMessage().contains("401"));
    } catch (IOException other) {
      fail("expected RejectedException but got " + other);
    }
  }

  @Test
  public void serverErrorFromTheRouteIsRetriable() {
    routeStatus = 503;
    try {
      store().put("e1", "d1", new byte[] {1});
      fail();
    } catch (BodyStore.RejectedException notExpected) {
      fail("a 503 must be retriable");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("503"));
    }
  }

  @Test
  public void failedUploadNamesTheKeyAndNeverTheUrl() {
    uploadStatus = 403;
    try {
      store().put("e1", "d1", new byte[] {1});
      fail();
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("oversize/kafka-connect/2026-11-17/d1/e1.json"));
      assertTrue(expected.getMessage().contains("403"));
      assertFalse(expected.getMessage().contains("X-Amz-Signature"));
    }
  }

  @Test
  public void responseWithoutAUrlIsAnError() {
    routeBody = "{\"bucket\":\"claims\"}";
    try {
      store().put("e1", "d1", new byte[] {1});
      fail();
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("no putUrl"));
    }
  }
}
