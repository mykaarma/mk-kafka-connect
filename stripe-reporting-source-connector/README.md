# Stripe Reporting Source Connector for Confluent

A Kafka Connect source connector that streams reporting data from Stripe API to Confluent Kafka topics.

## Overview

This connector provides a basic skeleton for integrating Stripe Reporting API with Kafka Connect. The current implementation is minimal and serves as a foundation for future development.

## Features

- Basic Kafka Connect source connector structure
- Minimal configuration for Stripe API integration
- HTTP client setup for Stripe API calls
- Extensible architecture for future enhancements

## Building the Connector

### Prerequisites

- Java 17 or higher
- Maven 3.6+

### Build Steps

```bash
# Clean and build the project
mvn clean package

# The connector JAR will be created at:
# target/stripe-reporting-source-connector-1.0.0.jar
```

## Configuration

### Required Properties

| Property | Description | Example |
|----------|-------------|---------|
| `stripe.api.keys` | Stripe API timezone to key mapping | `PST:sk_test_...;EST:sk_test_...` |

### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| `topic.prefix` | `stripe_reporting_` | Prefix for Kafka topics |
| `poll.cron` | `0 0 0 * * ?` (daily at midnight) | Cron expression for polling schedule (format: second minute hour day month weekday) |

### Example Configuration (Properties File)

```properties
name=stripe-source
connector.class=com.mykaarma.connect.stripe.StripeSourceConnector
tasks.max=1

# Stripe API
stripe.api.keys=MST:sk_test_...

# Topics
topic.prefix=stripe_reporting_

# Performance
# Cron expression examples:
# "0 */1 * * * ?" - every minute
# "0 0 * * * ?" - every hour
# "0 0 0 * * ?" - daily at midnight
poll.cron=0 0 0 * * ?
```

## Project Structure

```
mk-stripe-reporting-source-connector/
├── pom.xml
├── README.md
├── config/
│   └── stripe-source-connector.properties
└── src/
    └── main/
        ├── java/
        │   └── com/
        │       └── mykaarma/
        │           └── connect/
        │               └── stripe/
        │                   ├── StripeSourceConnector.java
        │                   ├── StripeSourceTask.java
        │                   ├── StripeSourceConnectorConfig.java
        │                   ├── StripeApiClient.java
        │                   └── StripeException.java
        └── resources/
            └── META-INF/
                └── services/
                    └── org.apache.kafka.connect.source.SourceConnector
```

## Development

### Code Structure

- `StripeSourceConnector`: Main connector class that manages configuration and task creation
- `StripeSourceTask`: Task implementation that polls Stripe API and produces records
- `StripeApiClient`: HTTP client for Stripe API interactions
- `StripeSourceConnectorConfig`: Configuration management
- `StripeException`: Exception from Stripe

## License

Copyright (c) 2025 MyKaarma. All rights reserved.
