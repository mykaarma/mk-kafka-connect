# Kafka Connect String Cast SMT

A simple and lightweight Kafka Connect Single Message Transform (SMT) that converts specified fields to strings in schemaless records.

## Overview

This SMT is designed to be a minimal, focused transformation that:
- Converts any field value to a string representation
- Works only with schemaless (Map-based) records
- Handles only record values (not keys)
- Silently skips fields that don't exist
- Supports complex types (Maps, Lists) using `.toString()`

## Features

- **Minimal code**: ~100 lines vs 600+ in Apache Kafka's Cast transformer
- **Simple configuration**: Just specify field names
- **Flexible**: Handles any input type (primitives, collections, maps)
- **Safe**: Silently skips non-existent fields
- **Schemaless only**: No schema handling overhead

## Configuration

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fields` | List | (required) | Comma-separated list of field names to convert to strings |

### Example Configuration

```properties
# Connector configuration
transforms=stringCast
transforms.stringCast.type=com.mykaarma.kafka.connect.transforms.StringCast$Value
transforms.stringCast.fields=userId,orderId,productId,metadata
```

### JSON Configuration (REST API)

```json
{
  "name": "my-connector",
  "config": {
    "connector.class": "...",
    "transforms": "stringCast",
    "transforms.stringCast.type": "com.mykaarma.kafka.connect.transforms.StringCast$Value",
    "transforms.stringCast.fields": "userId,orderId,productId"
  }
}
```

## Examples

### Input Record
```json
{
  "userId": 12345,
  "name": "John Doe",
  "orderId": 98765,
  "amount": 99.99,
  "active": true,
  "tags": ["premium", "verified"],
  "metadata": {
    "source": "mobile",
    "version": 2
  }
}
```

### Configuration
```properties
transforms.stringCast.fields=userId,orderId,amount,tags,metadata
```

### Output Record
```json
{
  "userId": "12345",
  "name": "John Doe",
  "orderId": "98765",
  "amount": "99.99",
  "active": true,
  "tags": "[premium, verified]",
  "metadata": "{source=mobile, version=2}"
}
```

## Building

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher

### Build Commands

```bash
# Clean and build
mvn clean package

# Run tests
mvn test

# Skip tests
mvn clean package -DskipTests
```

The compiled JAR will be located at `target/kafka-string-cast-smt-1.0.0.jar`

## Installation

### Option 1: Plugin Path (Recommended)

1. Build the JAR:
   ```bash
   mvn clean package
   ```

2. Create a plugin directory:
   ```bash
   mkdir -p /path/to/kafka-connect-plugins/string-cast-smt
   ```

3. Copy the JAR:
   ```bash
   cp target/kafka-string-cast-smt-1.0.0.jar /path/to/kafka-connect-plugins/string-cast-smt/
   ```

4. Configure Kafka Connect to use the plugin path in `connect-distributed.properties` or `connect-standalone.properties`:
   ```properties
   plugin.path=/path/to/kafka-connect-plugins
   ```

5. Restart Kafka Connect

### Option 2: Classpath

Copy the JAR to Kafka Connect's classpath directory and restart.

### Verify Installation

Check if the transform is available:
```bash
curl http://localhost:8083/connector-plugins | jq
```

Look for `com.mykaarma.kafka.connect.transforms.StringCast$Value` in the output.

## Usage Notes

### Supported Input Types
- Primitives: `int`, `long`, `double`, `float`, `boolean`
- Strings: passed through unchanged
- Collections: `List`, `Set` → converted using `.toString()`
- Maps: `Map` → converted using `.toString()`
- Null values: remain null

### Behavior
- **Non-existent fields**: Silently skipped (no error thrown)
- **Null values**: Field remains null (not converted to "null" string)
- **Already strings**: Passed through unchanged
- **Records with schema**: Ignored (transformation skipped)
- **Non-Map values**: Transformation skipped with warning

### Limitations
- Only works with schemaless records (no schema support)
- Only transforms values (keys not supported)
- No customization of string conversion logic
- Complex types use default `.toString()` representation

## Testing

Run the included unit tests:
```bash
mvn test
```

Tests cover:
- Simple field conversion (integers, longs, booleans)
- Non-existent field handling
- Null value handling
- Complex type conversion (Lists, Maps)
- Null field values
- Already-string fields

## Troubleshooting

### Transform not applied
- Check that the record is schemaless (no schema defined)
- Verify the field names match exactly (case-sensitive)
- Check Kafka Connect logs for warnings

### Fields not converted
- Ensure the field exists in the record
- Verify the transform is configured correctly
- Check that the transform is in the `transforms` chain

### ClassNotFoundException
- Verify the JAR is in the correct plugin path
- Restart Kafka Connect after adding the JAR
- Check `plugin.path` configuration

## Comparison with Apache Kafka Cast Transform

| Feature | Apache Cast | String Cast SMT |
|---------|-------------|-----------------|
| Lines of code | ~600 | ~100 |
| Schema support | ✓ | ✗ |
| Multiple target types | ✓ | ✗ (string only) |
| Whole-record casting | ✓ | ✗ |
| Schemaless support | ✓ | ✓ |
| Caching | ✓ | ✗ |
| Type validation | ✓ | ✗ |

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Tests pass: `mvn test`
- New features include tests
- Documentation is updated

## Support

For issues or questions:
- Check Kafka Connect logs
- Review the [configuration examples](#example-configuration)
- Verify [installation steps](#installation)


