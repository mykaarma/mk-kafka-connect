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
  public static final String BUCKET_CONFIG = "large.event.s3.bucket";
  public static final String REGION_CONFIG = "large.event.s3.region";
  public static final String PREFIX_CONFIG = "large.event.s3.prefix";
  public static final String ACCESS_KEY_CONFIG = "large.event.aws.access.key.id";
  public static final String SECRET_KEY_CONFIG = "large.event.aws.secret.access.key";

  static final String CLAIM_CHECK_FIELD = "claimCheck";
  static final String PROBE_KEY = "_probe";
  static final List<String> ENVELOPE_FIELDS = Arrays.asList(
      "dealerUuid", "dataType", "eventType", "eventId", "eventTimestamp", "departmentUuid");

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(LARGE_BYTES_CONFIG, ConfigDef.Type.INT, 800_000, ConfigDef.Range.atLeast(1),
          ConfigDef.Importance.HIGH,
          "Estimated serialized size above which a record is claim-checked.")
      .define(BUCKET_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "S3 bucket for oversized bodies.")
      .define(REGION_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "S3 bucket region.")
      .define(PREFIX_CONFIG, ConfigDef.Type.STRING, "claim-check", ConfigDef.Importance.MEDIUM,
          "Key prefix for oversized bodies.")
      .define(ACCESS_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Access key id of a user with s3:PutObject on the prefix.")
      .define(SECRET_KEY_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Secret access key.");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private int largeBytes;
  private String prefix;
  private BodyStore store;

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    largeBytes = config.getInt(LARGE_BYTES_CONFIG);
    prefix = trimSlashes(config.getString(PREFIX_CONFIG));
    store = createStore(config);
    try {
      store.put(prefix + "/" + PROBE_KEY, new byte[0]);
    } catch (IOException e) {
      throw new ConnectException("Probe upload to s3://" + store.bucket() + "/" + prefix
          + " failed, check the credential and bucket policy", e);
    }
  }

  BodyStore createStore(AbstractConfig config) {
    return new S3BodyStore(config.getString(BUCKET_CONFIG), config.getString(REGION_CONFIG),
        config.getString(ACCESS_KEY_CONFIG), config.getPassword(SECRET_KEY_CONFIG).value());
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
    String key = prefix + "/" + keyName(map.get("eventId"), body) + ".json";
    try {
      store.put(key, body);
    } catch (IOException e) {
      throw new RetriableException("Claim-check upload failed for " + key, e);
    }
    Map<String, Object> pointer = new LinkedHashMap<>();
    for (String field : ENVELOPE_FIELDS) {
      if (map.containsKey(field)) {
        pointer.put(field, map.get(field));
      }
    }
    Map<String, Object> claim = new LinkedHashMap<>();
    claim.put("bucket", store.bucket());
    claim.put("key", key);
    pointer.put(CLAIM_CHECK_FIELD, claim);
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
        record.key(), null, pointer, record.timestamp(), record.headers());
  }

  /** The event id makes a retry overwrite the same object, a content hash covers records without one. */
  static String keyName(Object eventId, byte[] body) {
    if (eventId != null && !eventId.toString().isEmpty()) {
      return eventId.toString().replaceAll("[^A-Za-z0-9._-]", "_");
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

  private static String trimSlashes(String s) {
    return s.replaceAll("^/+|/+$", "");
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}
}
