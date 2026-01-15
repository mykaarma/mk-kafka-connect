# Stripe Reporting Connector - Recovery Action Plan

## Problem Summary

The Stripe reporting connector experienced timeout errors due to a small configured value of `stripe.report.max.wait.ms`. When timeouts occurred, the connector failed to update offsets, leaving some timezones stuck with stale dates in the past.

### Root Cause

1. **Timeout Exception**: When `waitForReportCompletion()` exceeds `stripe.report.max.wait.ms`, it throws a `StripeException` (see `StripeApiClient.java:140-143`)
2. **No State Update**: When the exception bubbles up to `getRemainingRecordsForRound()`, it's caught as a generic exception and returns null, preventing any offset commit
3. **Stale State**: The old offset with past dates remains in Kafka Connect's offset storage
4. **Infinite Loop**: On next poll, the connector tries to resume from the stale offset, causing the same timeout again

## Recovery Steps

### Step 1: Increase Timeout Configuration (IMMEDIATE)

**Action**: Update the connector configuration to increase `stripe.report.max.wait.ms`

**Recommended Value**: 
- **Minimum**: `1800000` (30 minutes)
- **Recommended**: `3600000` (60 minutes) or higher
- **Current Default**: `300000` (5 minutes)

**Configuration Update**:
```json
{
  "stripe.report.max.wait.ms": "3600000"
}
```

**Why**: Stripe report generation can take significant time, especially for large date ranges. A 30-60 minute timeout provides sufficient buffer.

### Step 2: Identify Stuck Timezones

**Action**: Query Kafka Connect offset storage to identify timezones with stale dates

**Method 1: Using Kafka Connect REST API**

```bash
# Get connector name (replace with actual connector name)
CONNECTOR_NAME="stripe-reporting-source-connector"

# Query offset storage (requires access to Kafka Connect cluster)
# This will show all partitions and their offsets
curl -X GET "http://kafka-connect-host:8083/connectors/${CONNECTOR_NAME}/status"
```

**Method 2: Query Offset Topic Directly**

The offsets are stored in Kafka Connect's internal offset topic (typically `__connect-offsets` or similar). You can query this topic to find stale offsets:

```bash
# List all offsets for the connector
kafka-console-consumer \
  --bootstrap-server <kafka-broker> \
  --topic __connect-offsets \
  --from-beginning \
  --property print.key=true \
  --property print.value=true | \
  grep "stripe-reporting-source-connector"
```

**Method 3: Use Recovery Script**

See `scripts/identify_stuck_timezones.sh` (to be created) for automated detection.

**What to Look For**:
- `interval_start` and `interval_end` timestamps that are more than 7 days old
- `in_progress_report_run_id` that has been stuck for extended periods
- Timezones that haven't updated their `round_id` in recent cron cycles

### Step 3: Reset Stale Offsets

**Option A: Reset to Current Date (Recommended for Recovery)**

Reset stuck timezones to start from the current date, allowing them to catch up:

```bash
# Use the reset script (to be created)
./scripts/reset_stale_offsets.sh \
  --connector stripe-reporting-source-connector \
  --timezone "America/New_York" \
  --report-type "balance.summary.1" \
  --reset-to-current
```

**Option B: Reset to Latest Available Data**

Use Stripe API's `data_available_end` to reset to the latest available data point:

```bash
./scripts/reset_stale_offsets.sh \
  --connector stripe-reporting-source-connector \
  --timezone "America/New_York" \
  --report-type "balance.summary.1" \
  --reset-to-available-end
```

**Option C: Manual Offset Reset via Kafka Connect REST API**

```bash
# Delete specific partition offset
curl -X DELETE "http://kafka-connect-host:8083/connectors/${CONNECTOR_NAME}/offsets" \
  -H "Content-Type: application/json" \
  -d '{
    "partition": {
      "report_type": "balance.summary.1",
      "timezone": "America/New_York"
    }
  }'
```

**Note**: After resetting offsets, the connector will start from the reset point on the next poll cycle.

### Step 4: Verify Recovery

**Checkpoints**:

1. **Monitor Logs**: Check connector logs for successful report processing
   ```bash
   # Look for successful processing messages
   grep "Generated.*SourceRecords" connector.log | tail -20
   ```

2. **Check Offset Updates**: Verify offsets are updating with current timestamps
   ```bash
   # Query offsets again to verify they're updating
   curl -X GET "http://kafka-connect-host:8083/connectors/${CONNECTOR_NAME}/status"
   ```

3. **Verify Data Flow**: Check that records are being produced to Kafka topics
   ```bash
   # Check topic messages
   kafka-console-consumer \
     --bootstrap-server <kafka-broker> \
     --topic <stripe-topic> \
     --from-beginning \
     --max-messages 10
   ```

4. **Monitor All Timezones**: Ensure all configured timezones are processing
   ```bash
   # List all timezones from config
   # Compare with active processing in logs
   ```

### Step 5: Prevent Future Issues

**Immediate Improvements**:

1. **Increase Default Timeout**: Update default in `StripeSourceConnectorConfig.java`:
   ```java
   .define(StripeSourceConnector.STRIPE_REPORT_MAX_WAIT_MS_CONFIG,
           ConfigDef.Type.LONG,
           3600000L,  // Changed from 300000L to 3600000L (60 minutes)
           ConfigDef.Importance.MEDIUM,
           "Maximum wait time in milliseconds for report run completion (default: 60 minutes)")
   ```

2. **Improve Error Handling**: See `IMPROVEMENTS.md` for code changes to handle timeouts gracefully

3. **Add Monitoring**: Set up alerts for:
   - Timeout exceptions in logs
   - Stale offsets (offsets not updated in 24+ hours)
   - Failed report runs

## Recovery Checklist

- [ ] Step 1: Increase `stripe.report.max.wait.ms` configuration
- [ ] Step 2: Identify all stuck timezones
- [ ] Step 3: Reset stale offsets for stuck timezones
- [ ] Step 4: Verify recovery (monitor for 24-48 hours)
- [ ] Step 5: Implement preventive measures

## Timeline

- **Immediate (0-2 hours)**: Increase timeout configuration
- **Short-term (2-4 hours)**: Identify and reset stuck timezones
- **Verification (24-48 hours)**: Monitor recovery
- **Long-term (1 week)**: Implement code improvements and monitoring

## Rollback Plan

If recovery causes issues:

1. **Restore Previous Configuration**: Revert `stripe.report.max.wait.ms` to previous value
2. **Restore Offsets**: If offsets were reset incorrectly, restore from backup (if available)
3. **Contact Support**: Escalate if data loss is suspected

## Notes

- Offsets are stored in Kafka Connect's internal offset storage (typically a Kafka topic)
- Resetting offsets will cause the connector to reprocess data from the reset point
- Ensure sufficient Kafka topic retention to handle reprocessing
- Monitor Kafka Connect task restarts during recovery
