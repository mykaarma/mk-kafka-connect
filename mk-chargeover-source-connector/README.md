# ChargeOver Source Connector for Confluent

A high-performance Kafka Connect source connector that streams change data from ChargeOver to Confluent Kafka topics with intelligent cron-based scheduling and continuous pagination.

## Overview

This connector efficiently polls the ChargeOver API for changes to entities (customers, invoices, payments, subscriptions) and publishes them to Kafka topics. It uses a smart two-phase loading approach with cron-scheduled incremental loads and automatic gap recovery.

## Features

- ✅ **Change Data Capture (CDC)**: Tracks modifications to ChargeOver entities using configurable datetime field polling
- ✅ **Cron-Based Scheduling**: Flexible incremental loading with cron expressions (e.g., daily at 2 AM)
- ✅ **Continuous Pagination**: Fetches all pages without delay for maximum throughput
- ✅ **Automatic Gap Recovery**: Never skips data - automatically catches up if connector was down
- ✅ **Initial Load Mode**: Efficiently loads historical data from start date to current date
- ✅ **Incremental Load Mode**: Scheduled loads from last processed date to current date
- ✅ **Configurable Entities**: Select which ChargeOver entities to track with per-entity start dates
- ✅ **Entity-Specific Query Parameters**: Add custom query params per entity (expand, fields, etc.)
- ✅ **Robust Retry Logic**: Configurable retries with exponential backoff for transient failures
- ✅ **Offset Management**: Maintains precise state to resume from exact position on restart
- ✅ **Topic Routing**: Automatically routes entities to separate topics with configurable prefix
- ✅ **Confluent Cloud Ready**: Packaged as a fat JAR for seamless deployment

## Architecture

```
ChargeOver API → Connector (Cron Scheduled) → Kafka Topics
                      ↓
              Offset Storage (Kafka)
```

### Components

1. **ChargeOverSourceConnector**: Main connector class that manages configuration and task creation
2. **ChargeOverSourceTask**: Task implementation that polls ChargeOver API and produces records
3. **ChargeOverApiClient**: HTTP client for ChargeOver API interactions with continuous pagination
4. **ChargeOverSourceConnectorConfig**: Configuration management with cron expression validation

## Loading Modes

The connector operates in two phases with seamless automatic transitions:

### 1. Initial Load (INITIAL_LOAD)

- **Trigger**: When connector starts for the first time or when `initial.datetime` is configured
- **Behavior**: Loads all data from `initial.datetime` (or current datetime if not specified) to current datetime
- **Pagination**: Fetches all pages continuously without delays (batch size: 500 by default)
- **Transition**: Automatically switches to Incremental Load mode when caught up to current datetime

**Example**: If `initial.datetime=customer:2024-01-01 00:00:00` is set on November 1, 2024 at 14:30:00:
- Loads all customer data from 2024-01-01 00:00:00 to 2024-11-01 14:30:00
- Uses continuous pagination to fetch all pages efficiently
- Stores 2024-11-01 14:30:00 as last processed datetime
- Switches to Incremental Load mode

### 2. Incremental Load (INCREMENTAL_LOAD)

- **Trigger**: Based on cron expression schedule (default: `0 0 0 * * ?` - daily at midnight)
- **Behavior**: Loads from last processed datetime (inclusive) to current datetime
- **Gap Recovery**: If connector was down, automatically loads all missed data in one batch
- **Pagination**: Same continuous pagination as initial load

**Example**: With `incremental.schedule.cron=0 0 2 * * ?` (daily at 2 AM):
- At 2:00 AM on November 2, loads data from last processed datetime to 2024-11-02 02:00:00
- If connector was down for 3 days, loads data for all 3 missed days on next run
- No data is ever skipped

## State Management

The connector maintains precise state for each entity in Kafka's offset storage:

| State Field | Type | Description |
|-------------|------|-------------|
| `load_mode` | String | Current phase (INITIAL_LOAD or INCREMENTAL_LOAD) |
| `last_processed_datetime` | String | Last datetime successfully processed (YYYY-MM-DD HH:MM:SS) |
| `batch_end_datetime` | String | End datetime for current batch (upper bound, YYYY-MM-DD HH:MM:SS) |
| `next_scheduled_run` | Long | Timestamp of next scheduled incremental load |
| `current_offset` | Integer | Pagination position within current batch |
| `is_processing_batch` | Boolean | Flag indicating active batch processing |
| `retry_count` | Integer | Number of retries attempted for current batch |

This state enables the connector to:
- Resume exactly where it left off after restart
- Never skip data even if connector was down
- Track incremental schedule independently per entity
- Handle failures gracefully with retry tracking

## Building the Connector

### Prerequisites

- Java 17 or higher
- Maven 3.6+

### Build Steps

```bash
# Clean and build the project
mvn clean package

# The connector JAR will be created at:
# target/mk-chargeover-source-connector-1.0.0-jar-with-dependencies.jar
```

## Configuration

### Required Properties

| Property | Description | Example |
|----------|-------------|---------|
| `chargeover.subdomain` | ChargeOver subdomain | `your-company` |
| `chargeover.username` | ChargeOver API username | `api-user` |
| `chargeover.password` | ChargeOver API password | `your-password` |

### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| `topic.prefix` | `chargeover` | Prefix for Kafka topics |
| `poll.interval.ms` | `60000` (1 min) | Polling interval to check for scheduled work |
| `batch.size` | `500` | Maximum records per API call |
| `incremental.schedule.cron` | `0 0 0 * * ?` (midnight) | Quartz cron expression for incremental loads |
| `chargeover.timezone` | `UTC` | Timezone for datetime operations and cron schedule (e.g., 'UTC', 'America/New_York', 'Europe/London') |
| `chargeover.incremental.datetime.fields` | `customer:mod_datetime,invoice:mod_datetime,...` | Entity to datetime field mappings (e.g., customer:mod_datetime,invoice:updated_at) |
| `max.retries` | `3` | Maximum retries for failed API requests |
| `chargeover.entities` | `customer,invoice,payment,subscription` | Entities to track |
| `chargeover.entity.id.fields` | `customer:customer_id,invoice:invoice_id,...` | Entity to ID field mappings |
| `initial.datetime` | _(current datetime)_ | Start datetimes per entity in YYYY-MM-DD HH:MM:SS format (uses chargeover.timezone) |
| `chargeover.{entity}.query.params` | _(none)_ | Additional query parameters per entity (e.g., `chargeover.customer.query.params=expand=customer_external&fields=id,name`) |

### Cron Expression Format

Cron expressions follow Quartz Scheduler format: `second minute hour day month dayOfWeek`

**Common Examples**:
```properties
# Daily at midnight
incremental.schedule.cron=0 0 0 * * ?

# Daily at 2:00 AM
incremental.schedule.cron=0 0 2 * * ?

# Every Monday at midnight
incremental.schedule.cron=0 0 0 ? * 1

# Every 6 hours
incremental.schedule.cron=0 0 */6 * * ?
```

### Entity-Specific Query Parameters

You can add custom query parameters to API calls for specific entities using the pattern `chargeover.{entity}.query.params`. These parameters are appended to the API URL after the standard pagination and filtering parameters.

**Use Cases**:
- **Expand relationships**: Load related data in a single request
- **Field filtering**: Request only specific fields to reduce payload size
- **Custom filters**: Add entity-specific filtering beyond datetime ranges

**Examples**:
```properties
# Expand customer relationships
chargeover.customer.query.params=expand=customer_external&fields=id,name,email

# Expand invoice items and customer data
chargeover.invoice.query.params=expand=invoice_items,customer

# Request only specific payment fields
chargeover.payment.query.params=fields=payment_id,amount,status,mod_datetime
```

**Resulting API URL**:
```
https://your-company.chargeover.com/api/v3/customer?limit=500&offset=0&where=mod_datetime:GTE:...&order=mod_datetime:ASC&expand=customer_external&fields=id,name,email
```

**Note**: Query parameters are entity-specific and optional. Entities without configured query params use only the standard pagination and filtering parameters.

### Example Configuration (Confluent Cloud JSON)

```json
{
  "name": "chargeover-source",
  "config": {
    "connector.class": "com.mykaarma.connect.chargeover.ChargeOverSourceConnector",
    "tasks.max": "1",
    "chargeover.subdomain": "your-company",
    "chargeover.username": "api-user",
    "chargeover.password": "your-password",
    "topic.prefix": "chargeover",
    "poll.interval.ms": "60000",
    "batch.size": "500",
    "incremental.schedule.cron": "0 0 2 * * ?",
    "chargeover.timezone": "UTC",
    "chargeover.incremental.datetime.fields": "customer:mod_datetime,invoice:mod_datetime,payment:mod_datetime,subscription:mod_datetime",
    "max.retries": "3",
    "chargeover.entities": "customer,invoice,payment,subscription",
    "chargeover.entity.id.fields": "customer:customer_id,invoice:invoice_id,payment:payment_id,subscription:subscription_id",
    "initial.datetime": "customer:2024-01-01 00:00:00,invoice:2024-01-01 00:00:00,payment:2024-01-01 00:00:00,subscription:2024-01-01 00:00:00",
    "chargeover.customer.query.params": "expand=customer_external&fields=id,name,email",
    "chargeover.invoice.query.params": "expand=invoice_items,customer"
  }
}
```

### Example Configuration (Properties File)

```properties
name=chargeover-source
connector.class=com.mykaarma.connect.chargeover.ChargeOverSourceConnector
tasks.max=1

# ChargeOver API
chargeover.subdomain=your-company
chargeover.username=api-user
chargeover.password=your-password

# Topics
topic.prefix=chargeover

# Performance
poll.interval.ms=60000
batch.size=500
max.retries=3

# Scheduling
incremental.schedule.cron=0 0 2 * * ?
chargeover.timezone=UTC

# Datetime field mappings for each entity (optional - defaults to mod_datetime if not specified)
# Format: entity:field,entity:field,...
chargeover.incremental.datetime.fields=customer:mod_datetime,invoice:mod_datetime,payment:mod_datetime,subscription:mod_datetime

# Initial datetimes (optional - defaults to current datetime if not specified)
# Format: YYYY-MM-DD HH:MM:SS (uses timezone from chargeover.timezone)
initial.datetime=customer:2024-01-01 00:00:00,invoice:2024-01-01 00:00:00,payment:2024-01-01 00:00:00,subscription:2024-01-01 00:00:00

# Entities
chargeover.entities=customer,invoice,payment,subscription
chargeover.entity.id.fields=customer:customer_id,invoice:invoice_id,payment:payment_id,subscription:subscription_id

# Entity-specific query parameters (optional - adds custom query params to API calls)
chargeover.customer.query.params=expand=customer_external&fields=id,name,email
chargeover.invoice.query.params=expand=invoice_items,customer
```

## Deploying to Confluent Cloud

### Step 1: Package the Connector

```bash
mvn clean package
```

This creates a fat JAR with all dependencies: `target/mk-chargeover-source-connector-1.0.0-jar-with-dependencies.jar`

### Step 2: Upload to Confluent Cloud

1. Log in to [Confluent Cloud Console](https://confluent.cloud/)
2. Navigate to your cluster
3. Go to **Connectors** → **Add Plugin**
4. Upload the JAR file
5. Give it a name like "ChargeOver Source Connector"

### Step 3: Create Connector Instance

1. Click **Add Connector**
2. Select your uploaded ChargeOver connector plugin
3. Configure:
   - ChargeOver credentials (subdomain, username, password)
   - Entities to track
   - Initial dates (optional)
   - Cron schedule for incremental loads
4. Click **Launch**

## Topic Structure

Topics are created automatically with the following naming pattern:

```
{topic.prefix}.{entity}
```

**Examples**:
- `chargeover.customer`
- `chargeover.invoice`
- `chargeover.payment`
- `chargeover.subscription`

## Record Format

### Key
- **Type**: String
- **Value**: Entity ID from ChargeOver (e.g., customer_id, invoice_id)

### Value
- **Type**: JSON (schemaless)
- **Contents**: All fields from ChargeOver entity plus metadata

**Example Record**:
```json
{
  "customer_id": "12345",
  "mod_datetime": "2024-11-01 14:30:00",
  "name": "John Doe",
  "email": "john@example.com",
  "company": "Acme Corp",
  ...
  "_entity_type": "customer",
  "_ingestion_timestamp": 1730476800000,
  "_load_mode": "INCREMENTAL_LOAD"
}
```

**Metadata Fields**:
- `_entity_type`: Entity name (customer, invoice, payment, subscription)
- `_ingestion_timestamp`: Unix timestamp (ms) when record was ingested
- `_load_mode`: Loading mode when record was processed (INITIAL_LOAD or INCREMENTAL_LOAD)

## Monitoring

### Connector Health

Monitor in Confluent Cloud console:
- Task status and health
- Error messages and stack traces
- Record throughput and lag
- Last successful poll time

### Log Messages

Key log messages to watch:

**Initialization**:
```
Starting fresh initial load for entity: customer from 2024-01-01
```

**Progress**:
```
Entity: customer - fetched 500 records at offset 0, more available
Entity: customer - completed batch, processed up to 2024-11-01
```

**Mode Transitions**:
```
Initial load complete for entity: customer. Switching to incremental mode.
Entity: customer - next scheduled run at 2024-11-02 02:00:00
```

**Gap Recovery**:
```
Entity: customer - starting incremental load from 2024-10-29 to 2024-11-01
```

### Key Metrics

- **Records per batch**: Should match batch_size (500) when more data available
- **Batch completion time**: Measure end-to-end time for full batch
- **Next scheduled run**: Verify cron schedule is correct
- **Gap recovery**: Watch for date ranges when catching up

## Troubleshooting

### Connection Issues

**Problem**: Connector fails to connect to ChargeOver

**Solutions**:
1. Verify subdomain format: use `your-company` not full URL
2. Check API credentials are correct
3. Ensure ChargeOver API is accessible from Confluent Cloud
4. Check for IP allowlisting requirements
5. Verify API user has appropriate permissions

### No Records Produced

**Problem**: Connector running but no records appear

**Solutions**:
1. Check if data exists in ChargeOver for the datetime range
2. Verify `chargeover.entities` includes expected entities
3. Check `initial.datetime` - if not set, starts from current datetime
4. Review logs for current `last_processed_datetime`
5. Verify entity ID field mappings in configuration
6. Check offset storage for stuck state
7. Verify `chargeover.timezone` matches your expected timezone

### Initial Load Too Slow

**Problem**: Historical data load taking too long

**Solutions**:
1. Keep default `batch.size=500` for optimal performance
2. Check ChargeOver API response times
3. Review network latency between Confluent Cloud and ChargeOver
4. Consider setting `initial.datetime` closer to present if older data not needed
5. Monitor ChargeOver API rate limits

### Incremental Loads Not Running

**Problem**: Scheduled incremental loads not triggering

**Solutions**:
1. Verify cron expression syntax is correct
2. Check logs for `next_scheduled_run` timestamp
3. Ensure `poll.interval.ms` allows frequent checks
4. Verify connector is in INCREMENTAL_LOAD mode
5. Check for cron parsing errors in logs
6. Ensure connector wasn't restarted during scheduled time

### Data Gaps or Skipped Periods

**Problem**: Missing data for certain time periods

**Analysis**: This should not happen with the new design! The connector automatically catches up.

**If it does happen**:
1. Check logs for errors during that period
2. Verify `last_processed_date` in offset storage
3. Look for "too many consecutive failures" messages
4. Check ChargeOver API availability for that time range
5. Review retry count in offset storage

### Performance Issues

**Problem**: Connector slow or timing out

**Solutions**:
1. Keep `batch.size=500` (optimal value)
2. Adjust `poll.interval.ms` if checking too frequently
3. Check ChargeOver API rate limits and response times
4. Review network connectivity
5. Monitor JVM memory if processing large datasets
6. Consider using `tasks.max=1` (recommended)

### Cron Schedule Confusion

**Problem**: Incremental loads running at unexpected times

**Solutions**:
1. Use online cron expression validators (crontab.guru format differs from Quartz)
2. Check logs for "next scheduled run at" messages
3. Remember Quartz cron includes seconds field (first position)
4. Common mistake: Use `0 0 2 * * ?` not `0 2 * * *` (Quartz requires 6 fields with '?')

### State Reset Required

**Problem**: Need to restart from beginning or different datetime

**Solutions**:
1. Stop the connector
2. Delete connector offsets in Confluent Cloud
3. Update `initial.datetime` with new start datetimes
4. Restart connector - will begin initial load from new datetimes

## How It Works

### Continuous Pagination Flow

```
poll() called:
  ├─ Check if entity ready (schedule check)
  ├─ If ready, start batch
  │  ├─ Capture current datetime as batch_end_datetime
  │  ├─ Set offset = 0
  │  └─ Loop: fetch all pages
  │     ├─ Fetch page with filters (GTE: last_processed_datetime, LT: batch_end_datetime)
  │     ├─ Add records to batch
  │     ├─ Increment offset
  │     └─ Continue while hasMore
  ├─ Complete batch
  │  ├─ Store batch_end_datetime as last_processed_datetime
  │  └─ Calculate next_scheduled_run from cron
  └─ Return all records
```

### Initial Load Scenario

**Setup**: 
- `initial.datetime=customer:2024-01-01 00:00:00`
- Current datetime: 2024-11-01 14:30:00
- `batch.size=500`
- `incremental.schedule.cron=0 0 2 * * ?`
- `chargeover.timezone=UTC`

**Execution**:
1. **First poll**: Starts initial load from 2024-01-01 00:00:00 to 2024-11-01 14:30:00
   - API call example (assuming `chargeover.incremental.datetime.fields` maps customer to mod_datetime): 
     `mod_datetime:GTE:2024-01-01 00\:00\:00,mod_datetime:LT:2024-11-01 14\:30\:00`
2. **Pagination**: Fetches pages continuously
   - Offset 0-499: 500 records
   - Offset 500-999: 500 records
   - Offset 1000-1200: 200 records (last page)
3. **Completion**: Stores 2024-11-01 14:30:00 as last_processed_datetime
4. **Mode switch**: Changes to INCREMENTAL_LOAD
5. **Schedule**: Next run at 2024-11-02 02:00:00

### Incremental Load Scenario

**Setup**:
- Last processed: 2024-11-01
- Current date: 2024-11-02
- Time: 02:00:00 (cron trigger)

**Execution**:
1. **Trigger**: Cron schedule fires at 02:00:00
2. **Load**: Fetches from 2024-11-01 (inclusive) to 2024-11-02
3. **Pagination**: Continuous fetch of all pages
4. **Completion**: Stores 2024-11-02 as last_processed_date
5. **Schedule**: Next run at 2024-11-03 02:00:00

### Gap Recovery Scenario

**Setup**:
- Last processed: 2024-10-28
- Connector down: 2024-10-29 to 2024-10-31
- Restart: 2024-11-01 02:00:00

**Execution**:
1. **Trigger**: Cron schedule fires
2. **Gap detected**: Automatically loads from 2024-10-28 to 2024-11-01
3. **No data skipped**: All missed days included in one batch
4. **Pagination**: Continuous fetch of all pages for entire range
5. **Completion**: Stores 2024-11-01 as last_processed_date
6. **Back to normal**: Next run at 2024-11-02 02:00:00

### Failure Recovery

1. **Transient failure**: API request fails
2. **Retry logic**: Exponential backoff (1s, 2s, 4s, 8s, ...)
3. **Success**: Continues from same offset
4. **Max retries exceeded**: Logs error, keeps state for next poll
5. **Next poll**: Retries from same position (no data loss)

## Development

### Running Tests

```bash
mvn test
```

### Local Testing with Kafka Connect

1. **Build the connector**:
```bash
mvn clean package
```

2. **Copy to Kafka Connect plugins**:
```bash
cp target/mk-chargeover-source-connector-1.0.0-jar-with-dependencies.jar \
   /path/to/kafka/connect/plugins/
```

3. **Start Kafka Connect standalone**:
```bash
connect-standalone.sh \
  config/connect-standalone.properties \
  config/mk-chargeover-source-connector.properties
```

### Code Structure

```
src/main/java/com/mykaarma/connect/chargeover/
├── ChargeOverSourceConnector.java        # Main connector class
├── ChargeOverSourceTask.java             # Task with cron scheduling
├── ChargeOverApiClient.java              # HTTP client with pagination
└── ChargeOverSourceConnectorConfig.java  # Config with cron validation
```

### Best Practices

1. **Error Handling**: Always handle API failures gracefully
2. **Logging**: Use DEBUG for pagination, INFO for progress, ERROR for failures
3. **Testing**: Test cron expressions before deployment
4. **Configuration**: Always set explicit `initial.datetime` for production
5. **Monitoring**: Watch for next_scheduled_run in logs
6. **Timezone**: Ensure `chargeover.timezone` matches your business timezone

## ChargeOver API Reference

- **API Documentation**: [ChargeOver API Docs](https://developer.chargeover.com/)
- **Authentication**: HTTP Basic Auth (username:password)
- **Rate Limits**: Check your ChargeOver plan limits
- **Datetime Format**: YYYY-MM-DD HH:MM:SS for datetime field queries (colons must be escaped with backslash)
- **Datetime Fields**: Configurable per entity via `chargeover.incremental.datetime.fields` (defaults to mod_datetime)
- **API Filters**: GTE (>=) and LT (<) operators, separated by comma
- **Pagination**: Offset-based with limit parameter
- **Custom Query Params**: Add entity-specific parameters via `chargeover.{entity}.query.params` (e.g., expand, fields)

## License

Copyright (c) 2025 MyKaarma. All rights reserved.

