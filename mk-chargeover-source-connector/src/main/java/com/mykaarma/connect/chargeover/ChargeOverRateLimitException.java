package com.mykaarma.connect.chargeover;

import java.io.IOException;

/**
 * Exception thrown when ChargeOver API rate limit is exceeded (HTTP 429)
 */
public class ChargeOverRateLimitException extends IOException {
    public ChargeOverRateLimitException(String message) {
        super(message);
    }
}

