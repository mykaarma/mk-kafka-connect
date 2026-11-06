package com.mykaarma.kafka.connect.smt;

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
import java.util.HashSet;
import java.util.Set;

/** Calls the ops API subscription routes: version and snapshot. */
class SubscriptionClient {

  /** Every active (eventType, dataType, dealerUuid) key at one version. */
  static final class Snapshot {
    final long version;
    final long loadedAtEpochSec;
    final Set<String> keys;

    Snapshot(long version, long loadedAtEpochSec, Set<String> keys) {
      this.version = version;
      this.loadedAtEpochSec = loadedAtEpochSec;
      this.keys = keys;
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpClient http;
  private final String baseUrl;
  private final String authHeader;
  private final Duration readTimeout;

  SubscriptionClient(
      String baseUrl, String username, String password, int connectTimeoutMs, int readTimeoutMs) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.authHeader =
        "Basic "
            + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    this.readTimeout = Duration.ofMillis(readTimeoutMs);
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectTimeoutMs)).build();
  }

  long version() throws IOException {
    return get("/subscriptions/version").path("version").asLong();
  }

  Snapshot snapshot() throws IOException {
    JsonNode body = get("/subscriptions/snapshot");
    Set<String> keys = new HashSet<>();
    for (JsonNode k : body.path("keys")) {
      keys.add(key(k.path("eventType").asText(), k.path("dataType").asText(),
          k.path("dealerUuid").asText()));
    }
    return new Snapshot(body.path("version").asLong(), System.currentTimeMillis() / 1000, keys);
  }

  static String key(String eventType, String dataType, String dealerUuid) {
    return eventType + '\u001f' + dataType + '\u001f' + dealerUuid;
  }

  private JsonNode get(String path) throws IOException {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(baseUrl + path))
            .timeout(readTimeout)
            .header("Authorization", authHeader)
            .header("Accept", "application/json")
            .GET()
            .build();
    try {
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IOException("GET " + path + " returned HTTP " + response.statusCode());
      }
      return MAPPER.readTree(response.body());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted calling " + path, e);
    }
  }
}
