package com.mykaarma.connect.stripe;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mykaarma.connect.stripe.StripeSourceTask.ProcessingCombination;


import static org.junit.Assert.*;

/**
 * Integration test for StripeApiClient SourceRecord generation
 * 
 * This test uses a real Stripe API key and exercises the full flow:
 * - Creates a report run
 * - Waits for completion
 * - Downloads and parses CSV
 * - Generates SourceRecords
 * 
 * To run this test:
 * - Set system property: -Dstripe.test.api.key=sk_test_...
 * - Or set environment variable: STRIPE_TEST_API_KEY=sk_test_...
 * 
 * The test will be skipped if no API key is provided.
 */
public class StripeApiClientSourceRecordTest {
    
    private static final String SYSTEM_PROPERTY_API_KEY = "stripe.test.api.key";
    private static final String SYSTEM_PROPERTY_TIMEZONE = "stripe.test.timezone";
    private static final String ENV_VAR_API_KEY = "STRIPE_TEST_API_KEY";
    private static final String DEFAULT_REPORT_TYPE = "balance_change_from_activity.itemized.3";
    private static final String DEFAULT_TOPIC_PREFIX = "stripe_test_";
    private static final String DEFAULT_COLUMNS = "balance_transaction_id,created_utc,currency,gross,fee,net,reporting_category,description,automatic_payout_id,automatic_payout_effective_at_utc,customer_id,customer_name,shipping_address_state,charge_id,payment_intent_id,charge_created_utc,payment_method_type,card_brand,card_funding,payment_metadata[OrderNumber],connected_account_id,connected_account_name";
    private static final String DEFAULT_KEYS = "balance_transaction_id";
    private static final String DEFAULT_INITIAL_LOAD_DATES = "2025-11-27";

    
    private String apiKey;
    private StripeApiClient apiClient;
    private String timezone;
    private StripeSourceConnectorConfig config;

    private static final Logger log = LoggerFactory.getLogger(StripeApiClientSourceRecordTest.class);

    @Before
    public void setUp() throws Exception {
        // Read API key from system property or environment variable
        apiKey = System.getProperty(SYSTEM_PROPERTY_API_KEY);
        if (apiKey == null || apiKey.isEmpty()) {
            apiKey = System.getenv(ENV_VAR_API_KEY);
        }
        
        timezone = System.getProperty(SYSTEM_PROPERTY_TIMEZONE);
        if (timezone == null || timezone.isEmpty()) {
            timezone = "PST";
        }
        
        // Initialize API client if API key is available
        if (apiKey != null && !apiKey.isEmpty()) {
            // Use shorter timeouts for testing
            Map<String, String> props = new HashMap<>();
            props.put(StripeSourceConnector.STRIPE_TIMEZONE_APIKEY_MAPPING_CONFIG, timezone + ":" + apiKey);
            props.put(StripeSourceConnector.TOPIC_PREFIX_CONFIG, DEFAULT_TOPIC_PREFIX);
            props.put(StripeSourceConnector.POLL_CRON_CONFIG, "0 0 0 * * ?");
            props.put(StripeSourceConnector.BATCH_SIZE_CONFIG, "1000");
            props.put(StripeSourceConnector.STRIPE_REPORT_TYPES_CONFIG, DEFAULT_REPORT_TYPE);
            props.put(StripeSourceConnector.STRIPE_REPORT_COLUMNS_CONFIG, DEFAULT_REPORT_TYPE + ":" + DEFAULT_COLUMNS);
            props.put(StripeSourceConnector.STRIPE_REPORT_KEYS_CONFIG, DEFAULT_REPORT_TYPE + ":" + DEFAULT_KEYS);
            props.put(StripeSourceConnector.STRIPE_REPORT_INITIAL_LOAD_DATES_CONFIG, DEFAULT_REPORT_TYPE + ":" + DEFAULT_INITIAL_LOAD_DATES);
            props.put(StripeSourceConnector.STRIPE_REPORT_POLL_INTERVAL_MS_CONFIG, "5000");
            props.put(StripeSourceConnector.STRIPE_REPORT_MAX_WAIT_MS_CONFIG, "300000");
            props.put(StripeSourceConnector.IN_PROGRESS_POLL_INTERVAL_MS_CONFIG, "5000");
            props.put(StripeSourceConnector.MAX_REPORT_INTERVAL_DAYS_CONFIG, "2");
            config = new StripeSourceConnectorConfig(props);
            apiClient = new StripeApiClient(timezone, config);
        }
    }
    
    /**
     * Main integration test that verifies SourceRecord generation
     * 
     * To run this test, provide a Stripe API key:
     * - System property: -Dstripe.test.api.key=sk_test_...
     * - Environment variable: STRIPE_TEST_API_KEY=sk_test_...
     */
    @Ignore
    @Test
    public void testGenerateSourceRecords_Success() throws Exception {
        // Skip test if API key not provided (using JUnit Assume)
        Assume.assumeTrue("Stripe API key not provided. Set -Dstripe.test.api.key=sk_test_... or STRIPE_TEST_API_KEY=sk_test_... to run.",
                apiKey != null && !apiKey.isEmpty() && apiClient != null);
        
        long intervalEnd = apiClient.getDataAvailableEnd(DEFAULT_REPORT_TYPE) - 86400 * 2;
        long intervalStart = intervalEnd - 86400 * 1;
        
        System.out.println("Running integration test with:");
        System.out.println("  Report Type: " + DEFAULT_REPORT_TYPE);
        System.out.println("  Interval Start: " + intervalStart + " (" + new java.util.Date(intervalStart * 1000) + ")");
        System.out.println("  Interval End: " + intervalEnd + " (" + new java.util.Date(intervalEnd * 1000) + ")");

        // Generate SourceRecords
        // Call with null reportRunId/fileUrl (create new report), startRow=0, maxRecords=1000
        Long roundId = 10000L;
        List<SourceRecord> records = apiClient.generateSourceRecordsForSingleReportType(
                DEFAULT_REPORT_TYPE, 
                intervalStart, 
                intervalEnd,
                null,  // reportRunId - null to create new report
                0,     // startRow
                1000,  // maxRecords
                false, // isLastInterval
                roundId  // roundId
        );
        
        // Verify results
        assertNotNull("SourceRecords list should not be null", records);
        assertFalse("SourceRecords list should not be empty", records.isEmpty());
        
        System.out.println("Generated " + records.size() + " SourceRecords");
        
        // Verify each SourceRecord structure
        if (records.size() > 0) {
            SourceRecord record = records.get(0);
            System.out.println("SourceRecord: " + record.toString());
            record = records.get(records.size() - 1);
            System.out.println("Last SourceRecord: " + record.toString());
        }
    }

    @Ignore
    @Test
    public void testInitialPoll() throws Exception {
        Long currentRoundId = 10000L;
        List<ProcessingCombination> processingOrder = Collections.singletonList(new ProcessingCombination(timezone, DEFAULT_REPORT_TYPE));
        List<SourceRecord> records = getRemainingRecordsForRound(processingOrder, currentRoundId);
        assertNotNull("SourceRecords list should not be null", records);
        assertFalse("SourceRecords list should not be empty", records.isEmpty());
        System.out.println("Generated " + records.size() + " SourceRecords");
        
        // Verify each SourceRecord structure
        if (records.size() > 0) {
            SourceRecord record = records.get(0);
            System.out.println("SourceRecord: " + record.toString());
            record = records.get(records.size() - 1);
            System.out.println("Last SourceRecord: " + record.toString());
        }
    }

    private List<SourceRecord> getRemainingRecordsForRound(List<ProcessingCombination> processingOrder, Long roundId) throws Exception {
        List<SourceRecord> allRecords = new ArrayList<>();
        for (ProcessingCombination combo : processingOrder) {
            if (allRecords.size() >= config.getBatchSize()) {
                log.info("Reached batch size limit ({}), will resume round {} on next poll", config.getBatchSize(), roundId);
                return allRecords;
            }
            String reportType = combo.reportType;
            StripeSourceOffset offset = null;
            if (apiClient == null) {
                log.error("No API client found for timezone: {}, skipping", timezone);
                continue;
            }
            Long intervalStart = null;
            Long intervalEnd = null;
            String existingReportRunId = null;
            int lastCommittedRow = -1;
            if (offset == null || offset.getRoundId() == null) {
                // no offset found, or the round id is not set, starting from initial load date
                intervalStart = config.getInitialLoadDate(reportType);
                intervalEnd = apiClient.getDataAvailableEnd(reportType);
            }
            if (intervalStart == null || intervalEnd == null) {
                log.error("No interval start or end found for timezone {} report type {}, skipping", timezone, reportType);
                continue;
            }
            if (intervalStart >= intervalEnd) {
                log.debug("No new data available for timezone {} report type {}, skipping", timezone, reportType);
                continue;
            }
            boolean isLastInterval = true;
            Long maxDaysInSeconds = config.getMaxReportIntervalDays() * 24L * 60L * 60L;
            if (intervalStart + maxDaysInSeconds < intervalEnd) {
                intervalEnd = intervalStart + maxDaysInSeconds;
                isLastInterval = false;
            }
            List<SourceRecord> records = apiClient.generateSourceRecordsForSingleReportType(
                reportType, intervalStart, intervalEnd, existingReportRunId, lastCommittedRow + 1, config.getBatchSize() - allRecords.size(), isLastInterval, roundId);
            log.info("SourceTask got {} records for round {} report type {} timezone {}", records.size(), roundId, reportType, timezone);
            allRecords.addAll(records);
        }
        return allRecords;
    }

}