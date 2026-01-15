package com.mykaarma.connect.stripe;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

import static java.util.Collections.emptyList;

/**
 * Configuration class for Stripe Reporting Source Connector
 */
public class StripeSourceConnectorConfig extends AbstractConfig {
    
    public StripeSourceConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }
    
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(StripeSourceConnector.STRIPE_TIMEZONE_APIKEY_MAPPING_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Timezone to API key mapping. Format: 'timezone1:apikey1;timezone2:apikey2' (e.g., 'PST:sk_live_xxx;EST:sk_live_yyy')")
            .define(StripeSourceConnector.TOPIC_PREFIX_CONFIG,
                    ConfigDef.Type.STRING,
                    "stripe_reporting_",
                    ConfigDef.Importance.MEDIUM,
                    "Topic prefix")
            .define(StripeSourceConnector.POLL_CRON_CONFIG,
                    ConfigDef.Type.STRING,
                    "0 0 0 * * ?",
                    ConfigDef.Importance.MEDIUM,
                    "Cron expression for polling schedule. Format: second minute hour day month weekday")
            .define(StripeSourceConnector.BATCH_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    1000,
                    ConfigDef.Range.between(1, 10000),
                    ConfigDef.Importance.MEDIUM,
                    "Batch size for API requests")
            .define(StripeSourceConnector.STRIPE_REPORT_TYPES_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.Importance.HIGH,
                    "Comma-separated list of Stripe report types (e.g., 'balance.summary.1,card_payments_fees.transaction_level.2')")
            .define(StripeSourceConnector.STRIPE_REPORT_COLUMNS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Columns mapping per report type. Format: 'reportType1:col1,col2,col3;reportType2:col4,col5,col6'. " +
                    "If not specified, default report columns used by Stripe will be included.")
            .define(StripeSourceConnector.STRIPE_REPORT_KEYS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Keys mapping per report type. Format: 'reportType1:key1,key2,key3;reportType2:key4,key5,key6'. " +
                    "If not specified, default report keys used by Stripe will be included.")
            .define(StripeSourceConnector.STRIPE_REPORT_INITIAL_LOAD_DATES_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Initial load dates per report type. Format: 'reportType1:YYYY-MM-DD;reportType2:YYYY-MM-DD'. " +
                    "Used as fallback when no offset is found. Date should be in UTC. " +
                    "If not specified, defaults to day before yesterday.")
            .define(StripeSourceConnector.STRIPE_REPORT_POLL_INTERVAL_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    5000L,
                    ConfigDef.Importance.MEDIUM,
                    "Polling interval in milliseconds for checking report run status")
            .define(StripeSourceConnector.STRIPE_REPORT_MAX_WAIT_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    300000L,
                    ConfigDef.Importance.MEDIUM,
                    "Maximum wait time in milliseconds for report run completion (default: 5 minutes)")
            .define(StripeSourceConnector.IN_PROGRESS_POLL_INTERVAL_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    5000L,
                    ConfigDef.Importance.MEDIUM,
                    "Poll interval in milliseconds when reports are in progress (default: 5 seconds)")
            .define(StripeSourceConnector.MAX_REPORT_INTERVAL_DAYS_CONFIG,
                    ConfigDef.Type.INT,
                    30,
                    ConfigDef.Range.between(1, 365),
                    ConfigDef.Importance.MEDIUM,
                    "Maximum number of days for a single report run interval. If interval exceeds this, it will be split with remaining interval marked as in-progress (default: 30 days)");

    }
    
    /**
     * Get timezone to API key mapping
     * 
     * @return Map of timezone to API key
     */
    public Map<String, String> getTimezoneApiKeyMapping() {
        Map<String, String> result = new HashMap<>();
        
        String mapping = getPassword(StripeSourceConnector.STRIPE_TIMEZONE_APIKEY_MAPPING_CONFIG).value();
        if (mapping != null && !mapping.trim().isEmpty()) {
            // Parse format: "timezone1:apikey1;timezone2:apikey2"
            String[] mappings = mapping.split(";");
            for (String entry : mappings) {
                String[] parts = entry.trim().split(":", 2);
                if (parts.length == 2) {
                    String timezone = parts[0].trim();
                    String apiKey = parts[1].trim();
                    if (!timezone.isEmpty() && !apiKey.isEmpty()) {
                        result.put(timezone, apiKey);
                    }
                }
            }
        }
        return result;
    }
    
    public String getTopicPrefix() {
        return getString(StripeSourceConnector.TOPIC_PREFIX_CONFIG);
    }
    
    public String getPollCron() {
        return getString(StripeSourceConnector.POLL_CRON_CONFIG);
    }
    
    public Integer getBatchSize() {
        return getInt(StripeSourceConnector.BATCH_SIZE_CONFIG);
    }
    
    /**
     * Get list of report types to process
     * 
     * @return List of report type strings
     */
    public List<String> getReportTypes() {
        return getList(StripeSourceConnector.STRIPE_REPORT_TYPES_CONFIG);
    }
    
    /**
     * Get columns for a specific report type
     * 
     * @param reportType The report type
     * @return List of column names, or empty list if not specified
     */
    public List<String> getColumns(String reportType) {
        String columnsMapping = getString(StripeSourceConnector.STRIPE_REPORT_COLUMNS_CONFIG);
        if (columnsMapping == null || columnsMapping.trim().isEmpty()) {
            return emptyList();
        }
        
        // Parse format: "reportType1:col1,col2,col3;reportType2:col4,col5,col6"
        String[] mappings = columnsMapping.split(";");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":", 2);
            if (parts.length == 2 && parts[0].trim().equals(reportType)) {
                // Found matching report type, parse columns
                String[] columns = parts[1].split(",");
                List<String> columnList = new ArrayList<>();
                for (String column : columns) {
                    String trimmed = column.trim();
                    if (!trimmed.isEmpty()) {
                        columnList.add(trimmed);
                    }
                }
                return columnList;
            }
        }
        
        // No columns specified for this report type
        return emptyList();
    }
    
    /**
     * Get key columns for a specific report type
     * 
     * @param reportType The report type
     * @return List of key column names, or empty list if not specified
     */
    public List<String> getKeys(String reportType) {
        String keysMapping = getString(StripeSourceConnector.STRIPE_REPORT_KEYS_CONFIG);
        if (keysMapping == null || keysMapping.trim().isEmpty()) {
            return emptyList();
        }
        
        // Parse format: "reportType1:key1,key2,key3;reportType2:key4,key5,key6"
        String[] mappings = keysMapping.split(";");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":", 2);
            if (parts.length == 2 && parts[0].trim().equals(reportType)) {
                // Found matching report type, parse keys
                String[] keys = parts[1].split(",");
                List<String> keyList = new ArrayList<>();
                for (String key : keys) {
                    String trimmed = key.trim();
                    if (!trimmed.isEmpty()) {
                        keyList.add(trimmed);
                    }
                }
                return keyList;
            }
        }
        
        // No keys specified for this report type
        return emptyList();
    }
    
    /**
     * Get all report type to columns mappings
     * 
     * @return Map of report type to list of columns
     */
    public Map<String, List<String>> getReportTypeColumns() {
        Map<String, List<String>> result = new HashMap<>();
        
        String columnsMapping = getString(StripeSourceConnector.STRIPE_REPORT_COLUMNS_CONFIG);
        if (columnsMapping == null || columnsMapping.trim().isEmpty()) {
            return result;
        }
        
        // Parse format: "reportType1:col1,col2,col3;reportType2:col4,col5,col6"
        String[] mappings = columnsMapping.split(";");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":", 2);
            if (parts.length == 2) {
                String reportType = parts[0].trim();
                String[] columns = parts[1].split(",");
                List<String> columnList = new ArrayList<>();
                for (String column : columns) {
                    String trimmed = column.trim();
                    if (!trimmed.isEmpty()) {
                        columnList.add(trimmed);
                    }
                }
                if (!columnList.isEmpty()) {
                    result.put(reportType, columnList);
                }
            }
        }
        
        return result;
    }
    
    /**
     * Get initial load date for a specific report type
     * 
     * @param reportType The report type
     * @return UTC epoch timestamp in seconds for the beginning of the configured date, or null if not specified
     */
    public Long getInitialLoadDate(String reportType) {
        String initialLoadDates = getString(StripeSourceConnector.STRIPE_REPORT_INITIAL_LOAD_DATES_CONFIG);
        if (initialLoadDates == null || initialLoadDates.trim().isEmpty()) {
            return null;
        }
        
        // Parse format: "reportType1:YYYY-MM-DD;reportType2:YYYY-MM-DD"
        String[] mappings = initialLoadDates.split(";");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":", 2);
            if (parts.length == 2 && parts[0].trim().equals(reportType)) {
                // Found matching report type, parse date
                String dateStr = parts[1].trim();
                try {
                    // Parse date in format YYYY-MM-DD
                    java.time.LocalDate date = java.time.LocalDate.parse(dateStr);
                    // Convert to UTC epoch timestamp at beginning of day (00:00:00 UTC)
                    java.time.ZonedDateTime zonedDateTime = date.atStartOfDay(java.time.ZoneOffset.UTC);
                    return zonedDateTime.toEpochSecond();
                } catch (Exception e) {
                    // Invalid date format, return null
                    return null;
                }
            }
        }
        
        // No initial load date specified for this report type
        return null;
    }
    
    public Long getReportPollIntervalMs() {
        return getLong(StripeSourceConnector.STRIPE_REPORT_POLL_INTERVAL_MS_CONFIG);
    }
    
    public Long getReportMaxWaitMs() {
        return getLong(StripeSourceConnector.STRIPE_REPORT_MAX_WAIT_MS_CONFIG);
    }
    
    public Long getInProgressPollIntervalMs() {
        return getLong(StripeSourceConnector.IN_PROGRESS_POLL_INTERVAL_MS_CONFIG);
    }
    
    public Integer getMaxReportIntervalDays() {
        return getInt(StripeSourceConnector.MAX_REPORT_INTERVAL_DAYS_CONFIG);
    }
}

