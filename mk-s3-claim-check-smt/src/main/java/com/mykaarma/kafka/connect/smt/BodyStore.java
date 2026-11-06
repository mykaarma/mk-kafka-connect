package com.mykaarma.kafka.connect.smt;

import java.io.IOException;

/** Where oversized record bodies are written. */
interface BodyStore {
  void put(String key, byte[] body) throws IOException;

  String bucket();
}
