package com.mykaarma.kafka.connect.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes bodies to S3 through the ops API. POST /claim-check returns a presigned PUT URL for the
 * exact body length, and the body goes to that URL. The connector holds no AWS credential.
 */
class ApiBodyStore implements BodyStore {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PROBE_ID = "_probe";

  private final HttpClient http;
  private final String baseUrl;
  private final String authHeader;
  private final Duration readTimeout;
  private final Duration uploadTimeout;

  ApiBodyStore(String baseUrl, String username, String password, int connectTimeoutMs,
      int readTimeoutMs, int uploadTimeoutMs) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.authHeader = "Basic " + Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    this.readTimeout = Duration.ofMillis(readTimeoutMs);
    this.uploadTimeout = Duration.ofMillis(uploadTimeoutMs);
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectTimeoutMs)).build();
  }

  /** Asks for a URL and does not use it, so startup proves the route and the credential. */
  @Override
  public void probe() throws IOException {
    requestUrl(PROBE_ID, PROBE_ID, 1);
  }

  @Override
  public Location put(String eventId, String dealerUuid, byte[] body) throws IOException {
    JsonNode url = requestUrl(eventId, dealerUuid, body.length);
    String putUrl = url.path("putUrl").asText();
    String key = url.path("key").asText();
    if (putUrl.isEmpty() || key.isEmpty()) {
      throw new IOException("POST /claim-check returned no putUrl or key");
    }
    // The URL is signed for this length, and the JDK client sends exactly that Content-Length.
    HttpRequest upload = HttpRequest.newBuilder(URI.create(putUrl))
        .timeout(uploadTimeout)
        .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
        .build();
    HttpResponse<String> response = send(upload);
    if (response.statusCode() / 100 != 2) {
      // The URL is a credential, so the message carries the key and the status, never the URL.
      throw new IOException("Upload of " + key + " returned HTTP " + response.statusCode());
    }
    return new Location(url.path("bucket").asText(), key);
  }

  private JsonNode requestUrl(String eventId, String dealerUuid, int bytes) throws IOException {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("eventId", eventId);
    payload.put("dealerUuid", dealerUuid);
    payload.put("bytes", bytes);
    HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/claim-check"))
        .timeout(readTimeout)
        .header("Authorization", authHeader)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(toJson(payload)))
        .build();
    HttpResponse<String> response = send(request);
    int status = response.statusCode();
    if (status == 200) {
      try {
        return MAPPER.readTree(response.body());
      } catch (JsonProcessingException e) {
        // Plain IOException: Jackson is shaded into this jar, and an exception of its own would
        // not survive the trip out of Confluent's function runtime.
        throw new IOException("POST /claim-check returned malformed JSON: " + e.getOriginalMessage());
      }
    }
    String message = "POST /claim-check returned HTTP " + status;
    // A 4xx means this request is wrong or the credential is, so a retry repeats it.
    if (status >= 400 && status < 500) {
      throw new RejectedException(message);
    }
    throw new IOException(message);
  }

  private static String toJson(Map<String, Object> payload) throws IOException {
    try {
      return MAPPER.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IOException("Could not build the POST /claim-check body: " + e.getOriginalMessage());
    }
  }

  private HttpResponse<String> send(HttpRequest request) throws IOException {
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted calling " + request.uri().getPath(), e);
    }
  }
}
