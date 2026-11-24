package com.mykaarma.kafka.connect.smt;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops records for dealers with no active subscription, replacing the Confluent
 * Filter$Value dealer arrays. Holds a snapshot of every active (eventType, dataType,
 * dealerUuid) key from the ops API and reloads it when the subscription version moves.
 *
 * <p>When the ops API cannot be reached the filter fails open: it passes every record, and the
 * filtering Lambda, which checks subscriptions itself, does the real filtering. That covers a start-up
 * with no snapshot and any failed poll, so a subscription added during an outage is never dropped. It
 * filters again after the next successful load.
 *
 * <p>POC scope: snapshot lookup only. The per-key check route is not used.
 */
public class DealerSubscriptionFilter<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DealerSubscriptionFilter.class);

  public static final String BASE_URL_CONFIG = "ops.api.base.url";
  public static final String USERNAME_CONFIG = "ops.api.username";
  public static final String PASSWORD_CONFIG = "ops.api.password";
  public static final String DEALER_FIELD_CONFIG = "field.dealer.uuid";
  public static final String DATA_TYPE_FIELD_CONFIG = "field.data.type";
  public static final String EVENT_TYPE_FIELD_CONFIG = "field.event.type";
  public static final String POLL_INTERVAL_CONFIG = "poll.interval.ms";
  public static final String CONNECT_TIMEOUT_CONFIG = "http.connect.timeout.ms";
  public static final String READ_TIMEOUT_CONFIG = "http.read.timeout.ms";
  public static final String MODE_CONFIG = "mode";
  public static final String TIMING_WINDOW_CONFIG = "timing.log.interval.ms";

  static final String DEFAULT_EVENT_TYPE = "pointintime";
  /** A second change inside the same second as a read does not move the version. */
  static final long RESOLUTION_SEC = 2;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(BASE_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Kong base URL of the ops API, up to and including /webhook/v1.")
      .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Service subscriber user name.")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.HIGH, "Service subscriber password.")
      .define(DEALER_FIELD_CONFIG, ConfigDef.Type.STRING, "dealerUuid",
          ConfigDef.Importance.MEDIUM, "Path of the dealer uuid in the value.")
      .define(DATA_TYPE_FIELD_CONFIG, ConfigDef.Type.STRING, "dataType",
          ConfigDef.Importance.MEDIUM, "Path of the data type in the value.")
      .define(EVENT_TYPE_FIELD_CONFIG, ConfigDef.Type.STRING, "eventType",
          ConfigDef.Importance.MEDIUM,
          "Path of the event type in the value. Missing, null or empty means pointintime.")
      .define(POLL_INTERVAL_CONFIG, ConfigDef.Type.LONG, 10_000L, ConfigDef.Range.atLeast(1000),
          ConfigDef.Importance.MEDIUM, "How often the version route is polled.")
      .define(CONNECT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 1000, ConfigDef.Importance.LOW,
          "HTTP connect timeout.")
      .define(READ_TIMEOUT_CONFIG, ConfigDef.Type.INT, 2000, ConfigDef.Importance.LOW,
          "HTTP read timeout.")
      .define(MODE_CONFIG, ConfigDef.Type.STRING, "enforce",
          ConfigDef.ValidString.in("enforce", "shadow"), ConfigDef.Importance.MEDIUM,
          "enforce drops unsubscribed records. shadow never drops and logs the decision.")
      .define(TIMING_WINDOW_CONFIG, ConfigDef.Type.LONG, 60_000L, ConfigDef.Range.atLeast(0),
          ConfigDef.Importance.LOW,
          "Window of the apply() timing summary line in the log. 0 turns the timing off.");

  private String dealerField;
  private String dataTypeField;
  private String eventTypeField;
  private boolean shadow;
  private SubscriptionClient client;
  /** Null until the first load succeeds. */
  private volatile SubscriptionClient.Snapshot snapshot;
  /** True while the last load or poll failed: records pass unfiltered. */
  private volatile boolean failOpen;
  private ScheduledExecutorService poller;
  private ApplyTiming timing;

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    dealerField = config.getString(DEALER_FIELD_CONFIG);
    dataTypeField = config.getString(DATA_TYPE_FIELD_CONFIG);
    eventTypeField = config.getString(EVENT_TYPE_FIELD_CONFIG);
    shadow = "shadow".equals(config.getString(MODE_CONFIG));
    timing = new ApplyTiming("dealer-subscription", config.getLong(TIMING_WINDOW_CONFIG),
        System::nanoTime, LOG::info);
    Password password = config.getPassword(PASSWORD_CONFIG);
    client = new SubscriptionClient(config.getString(BASE_URL_CONFIG),
        config.getString(USERNAME_CONFIG), password.value(),
        config.getInt(CONNECT_TIMEOUT_CONFIG), config.getInt(READ_TIMEOUT_CONFIG));
    long loadStart = System.nanoTime();
    try {
      snapshot = client.snapshot();
      LOG.info("Loaded subscription snapshot version {} with {} keys, snapshot_ms={}",
          snapshot.version, snapshot.keys.size(), (System.nanoTime() - loadStart) / 1_000_000);
    } catch (IOException | RuntimeException e) {
      enterFailOpen(e);
    }
    long interval = config.getLong(POLL_INTERVAL_CONFIG);
    poller = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "dealer-subscription-poller");
      t.setDaemon(true);
      return t;
    });
    poller.scheduleWithFixedDelay(this::poll, interval, interval, TimeUnit.MILLISECONDS);
  }

  /** Reloads when the version moved, or when the snapshot was taken within the resolution window. */
  void poll() {
    try {
      SubscriptionClient.Snapshot current = snapshot;
      long t0 = System.nanoTime();
      long version = client.version();
      long versionMs = (System.nanoTime() - t0) / 1_000_000;
      boolean inWindow = current != null && current.loadedAtEpochSec - current.version <= RESOLUTION_SEC;
      if (current == null || version != current.version || inWindow) {
        long t1 = System.nanoTime();
        snapshot = client.snapshot();
        LOG.info("Reloaded subscription snapshot version {} with {} keys, version_ms={} "
            + "snapshot_ms={}", snapshot.version, snapshot.keys.size(), versionMs,
            (System.nanoTime() - t1) / 1_000_000);
      } else {
        LOG.info("Subscription poll: version {} unchanged, version_ms={}", version, versionMs);
      }
      if (failOpen) {
        failOpen = false;
        LOG.info("Subscription API reachable again, filtering resumed");
      }
    } catch (IOException | RuntimeException e) {
      enterFailOpen(e);
    }
  }

  private void enterFailOpen(Exception e) {
    if (!failOpen) {
      failOpen = true;
      LOG.warn("Subscription API unavailable, passing every record until it answers: {}",
          e.toString());
    }
  }

  @Override
  public R apply(R record) {
    if (!timing.enabled()) {
      return filter(record);
    }
    long started = timing.start();
    try {
      return filter(record);
    } finally {
      timing.end(started);
    }
  }

  private R filter(R record) {
    Object value = record.value();
    SubscriptionClient.Snapshot current = snapshot;
    if (value == null || failOpen || current == null) {
      return record;
    }
    String dealer = text(value, dealerField);
    String dataType = text(value, dataTypeField);
    String eventType = text(value, eventTypeField);
    if (eventType == null || eventType.isEmpty()) {
      eventType = DEFAULT_EVENT_TYPE;
    }
    boolean subscribed = dealer != null && dataType != null
        && current.keys.contains(SubscriptionClient.key(eventType, dataType, dealer));
    if (subscribed) {
      return record;
    }
    if (shadow) {
      LOG.info("shadow: would drop eventType={} dataType={} dealerUuid={}", eventType, dataType,
          dealer);
      return record;
    }
    return null;
  }

  /** Reads a dotted path from a Map or Struct value, as a string, or null when absent. */
  static String text(Object value, String path) {
    Object current = value;
    for (String part : path.split("\\.")) {
      if (current instanceof Map) {
        current = ((Map<?, ?>) current).get(part);
      } else if (current instanceof Struct) {
        Struct struct = (Struct) current;
        current = struct.schema().field(part) == null ? null : struct.get(part);
      } else if (current == null) {
        return null;
      } else {
        throw new DataException("Expected a Struct or a Map at '" + part + "' of path " + path
            + " but got " + current.getClass().getName());
      }
    }
    return current == null ? null : current.toString();
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    if (poller != null) {
      poller.shutdownNow();
    }
  }
}
