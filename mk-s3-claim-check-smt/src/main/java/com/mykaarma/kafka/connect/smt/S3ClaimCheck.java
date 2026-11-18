package com.mykaarma.kafka.connect.smt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Replaces a record that is too large for an async Lambda invoke with a small pointer record and
 * writes the full body to S3. Chain it after the dealer filter, so only subscribed records arrive.
 *
 * <p>The pointer record is a schemaless Map holding the envelope fields plus
 * claimCheck: {bucket, key}.
 */
public class S3ClaimCheck<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String LARGE_BYTES_CONFIG = "large.event.bytes";
  public static final String BASE_URL_CONFIG = "ops.api.base.url";
  public static final String USERNAME_CONFIG = "ops.api.username";
  public static final String PASSWORD_CONFIG = "ops.api.password";
  public static final String CONNECT_TIMEOUT_CONFIG = "http.connect.timeout.ms";
  public static final String READ_TIMEOUT_CONFIG = "http.read.timeout.ms";
  public static final String UPLOAD_TIMEOUT_CONFIG = "http.upload.timeout.ms";

  static final String CLAIM_CHECK_FIELD = "claimCheck";
  static final String UNKNOWN_DEALER = "unknown";
  static final List<String> ENVELOPE_FIELDS = Arrays.asList(
      "dealerUuid", "dataType", "eventType", "eventId", "eventTimestamp", "departmentUuid");

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(LARGE_BYTES_CONFIG, ConfigDef.Type.INT, 800_000, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.HIGH,
          "Estimated serialized size above which a record is claim-checked.")
      .define(BASE_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH,
          "Ops API base URL up to /webhook/v1, with no trailing slash. POST /claim-check issues "
              + "the upload URL.")
      .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Basic-auth username for the ops API.")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Basic-auth password for the ops API.")
      .define(CONNECT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 3000, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.LOW, "Connect timeout for the ops API call.")
      .define(READ_TIMEOUT_CONFIG, ConfigDef.Type.INT, 5000, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.LOW, "Read timeout for the ops API call.")
      .define(UPLOAD_TIMEOUT_CONFIG, ConfigDef.Type.INT, 30_000, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.LOW, "Timeout for uploading one body to the issued URL.");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private int largeBytes;
  private BodyStore store;

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    largeBytes = config.getInt(LARGE_BYTES_CONFIG);
    store = createStore(config);
    try {
      store.probe();
    } catch (IOException e) {
      // The SMT runs in Confluent's function runtime and the worker rebuilds what it throws, so
      // only JDK and Kafka classes may sit in the cause chain. RejectedException is ours.
      throw new ConnectException("Could not get a claim-check upload URL from "
          + config.getString(BASE_URL_CONFIG) + ", check the URL and the credential: "
          + e.getMessage(), e instanceof BodyStore.RejectedException ? null : e);
    }
  }

  BodyStore createStore(AbstractConfig config) {
    return new ApiBodyStore(config.getString(BASE_URL_CONFIG), config.getString(USERNAME_CONFIG),
        config.getPassword(PASSWORD_CONFIG).value(), config.getInt(CONNECT_TIMEOUT_CONFIG),
        config.getInt(READ_TIMEOUT_CONFIG), config.getInt(UPLOAD_TIMEOUT_CONFIG));
  }

  @Override
  public R apply(R record) {
    Object value = record.value();
    if (value == null) {
      return record;
    }
    Object plain = toPlain(value);
    byte[] body;
    try {
      body = MAPPER.writeValueAsBytes(plain);
    } catch (JsonProcessingException e) {
      throw new DataException("Could not serialize the record value to JSON", e);
    }
    if (body.length <= largeBytes) {
      return record;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) plain;
    String eventId = keyName(map.get("eventId"), body);
    String dealerUuid = safe(map.get("dealerUuid"), UNKNOWN_DEALER);
    BodyStore.Location location;
    try {
      location = store.put(eventId, dealerUuid, body);
    } catch (BodyStore.RejectedException e) {
      // No cause: the worker cannot load RejectedException, so it carries only its message.
      throw new DataException("Claim-check upload rejected for event " + eventId + ": "
          + e.getMessage());
    } catch (IOException e) {
      throw new RetriableException("Claim-check upload failed for event " + eventId, e);
    }
    Map<String, Object> pointer = new LinkedHashMap<>();
    for (String field : ENVELOPE_FIELDS) {
      if (map.containsKey(field)) {
        pointer.put(field, map.get(field));
      }
    }
    Map<String, Object> claim = new LinkedHashMap<>();
    claim.put("bucket", location.bucket);
    claim.put("key", location.key);
    pointer.put(CLAIM_CHECK_FIELD, claim);
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
        record.key(), null, pointer, record.timestamp(), record.headers());
  }

  /** Limits a value to what the ops API accepts in an object key: 1-128 of A-Z a-z 0-9 . _ - */
  static String safe(Object value, String fallback) {
    String s = value == null ? "" : value.toString().replaceAll("[^A-Za-z0-9._-]", "_");
    if (s.isEmpty()) {
      return fallback;
    }
    return s.length() > 128 ? s.substring(0, 128) : s;
  }

  /** The event id makes a retry overwrite the same object, a content hash covers records without one. */
  static String keyName(Object eventId, byte[] body) {
    if (eventId != null && !eventId.toString().isEmpty()) {
      return safe(eventId, "event");
    }
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(body);
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Converts a Struct tree to plain Maps and Lists. Maps and scalars pass through. */
  static Object toPlain(Object value) {
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Map<String, Object> out = new LinkedHashMap<>();
      for (Field f : struct.schema().fields()) {
        out.put(f.name(), toPlain(struct.get(f)));
      }
      return out;
    }
    if (value instanceof Map) {
      Map<String, Object> out = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
        out.put(String.valueOf(e.getKey()), toPlain(e.getValue()));
      }
      return out;
    }
    if (value instanceof List) {
      List<Object> out = new java.util.ArrayList<>();
      for (Object o : (List<?>) value) {
        out.add(toPlain(o));
      }
      return out;
    }
    if (value instanceof byte[]) {
      return java.util.Base64.getEncoder().encodeToString((byte[]) value);
    }
    return value;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}
}
