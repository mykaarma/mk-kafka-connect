package com.mykaarma.kafka.connect.smt;

import java.io.IOException;

/** Where oversized record bodies are written. */
interface BodyStore {

  /** Where one body landed. */
  final class Location {
    final String bucket;
    final String key;

    Location(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }
  }

  /** The store refused the request itself, so retrying the same body cannot help. */
  class RejectedException extends IOException {
    RejectedException(String message) {
      super(message);
    }
  }

  /** Fails at startup when the store cannot be reached or does not accept the credential. */
  void probe() throws IOException;

  Location put(String eventId, String dealerUuid, byte[] body) throws IOException;
}
