package com.mykaarma.kafka.connect.smt;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A thread-safe, size-bounded LRU cache. Evicts the least-recently-used entry
 * once {@code maxSize} is exceeded, so a task processing many distinct schemas
 * (e.g. across many topics with schema evolution) cannot grow its cache
 * without bound.
 */
final class BoundedCache {

  private BoundedCache() {}

  static <K, V> Map<K, V> create(int maxSize) {
    LinkedHashMap<K, V> delegate = new LinkedHashMap<K, V>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
      }
    };
    return Collections.synchronizedMap(delegate);
  }
}
