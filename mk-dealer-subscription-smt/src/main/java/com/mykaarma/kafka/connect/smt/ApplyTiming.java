package com.mykaarma.kafka.connect.smt;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Times each apply() call and reports one summary line per window. Confluent publishes no metric
 * for the time spent in a transform, so this is the only direct measure.
 *
 * <p>A task runs its transform chain on one thread, so nothing here is synchronized. The window is
 * closed by the next apply() call after it ends, so an idle task logs nothing. Percentiles come from
 * the first {@value #CAPACITY} calls of the window.
 */
final class ApplyTiming {

  static final int CAPACITY = 8192;

  private final String name;
  private final long windowNanos;
  private final LongSupplier clock;
  private final Consumer<String> sink;
  private final long[] samples = new long[CAPACITY];
  private int sampled;
  private long count;
  private long sumNanos;
  private long maxNanos;
  private long windowStart;

  /** A windowMs of 0 turns timing off. */
  ApplyTiming(String name, long windowMs, LongSupplier clock, Consumer<String> sink) {
    this.name = name;
    this.windowNanos = windowMs * 1_000_000L;
    this.clock = clock;
    this.sink = sink;
    this.windowStart = clock.getAsLong();
  }

  boolean enabled() {
    return windowNanos > 0;
  }

  long start() {
    return clock.getAsLong();
  }

  void end(long startNanos) {
    long now = clock.getAsLong();
    long elapsed = now - startNanos;
    count++;
    sumNanos += elapsed;
    maxNanos = Math.max(maxNanos, elapsed);
    if (sampled < CAPACITY) {
      samples[sampled++] = elapsed;
    }
    if (now - windowStart >= windowNanos) {
      flush(now);
    }
  }

  private void flush(long now) {
    long[] sorted = Arrays.copyOf(samples, sampled);
    Arrays.sort(sorted);
    sink.accept(String.format(
        "smt-timing smt=%s window_s=%.0f records=%d avg_us=%.1f p50_us=%.1f p95_us=%.1f "
            + "p99_us=%.1f max_us=%.1f",
        name, (now - windowStart) / 1e9, count, sumNanos / (double) count / 1000,
        percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99),
        maxNanos / 1000.0));
    count = 0;
    sumNanos = 0;
    maxNanos = 0;
    sampled = 0;
    windowStart = now;
  }

  private static double percentile(long[] sorted, double p) {
    return sorted[(int) Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)] / 1000.0;
  }
}
