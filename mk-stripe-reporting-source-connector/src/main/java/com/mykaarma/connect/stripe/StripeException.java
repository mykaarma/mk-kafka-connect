package com.mykaarma.connect.stripe;

/**
 * Exception thrown when Stripe API operations fail
 */
public class StripeException extends Exception {
    public StripeException(String message) {
        super(message);
    }
    
    public StripeException(String message, Throwable cause) {
        super(message, cause);
    }
}

