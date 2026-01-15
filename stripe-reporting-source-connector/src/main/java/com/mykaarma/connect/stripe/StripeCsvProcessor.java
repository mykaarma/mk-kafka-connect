package com.mykaarma.connect.stripe;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Handles CSV file downloading, parsing, and conversion to Kafka SourceRecords
 * Uses Apache Commons CSV for robust CSV parsing
 */
public class StripeCsvProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(StripeCsvProcessor.class);
    
    private final String apiKey;
    private final String topicPrefix;
    private final String reportType;
    private final String timezone;
    private final List<String> keyColumns;
    private final ObjectMapper objectMapper;
    private final CloseableHttpClient httpClient;
    private final DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Pattern datePattern = Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})");

    
    // CSV format with header support
    private static final CSVFormat CSV_FORMAT = CSVFormat.Builder.create(CSVFormat.DEFAULT)
            .setHeader()
            .setSkipHeaderRecord(true)
            .setTrim(true)
            .build();
    
    public StripeCsvProcessor(String apiKey, String topicPrefix, String reportType, String timezone, List<String> keyColumns) {
        this.apiKey = apiKey;
        this.topicPrefix = topicPrefix;
        this.reportType = reportType;
        this.timezone = timezone;
        this.keyColumns = keyColumns != null ? keyColumns : new ArrayList<>();
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClients.createDefault();
    }
    
    /**
     * Downloads CSV file from URL and converts it to SourceRecords
     * 
     * @param fileUrl The URL of the CSV file to download
     * @param reportRunId The report run ID for source partition/offset tracking
     * @param intervalStart The start timestamp of the report interval
     * @param intervalEnd The end timestamp of the report interval
     * @param startRow The row index to start processing from (0-based, for resuming)
     * @param maxRecords Maximum number of records to process in this batch
     * @return List of SourceRecords ready to be sent to Kafka
     * @throws Exception if file download or parsing fails
     */
    public List<SourceRecord> processCsvFile(String fileUrl, String reportRunId, long intervalStart, long intervalEnd, 
                                            int startRow, int maxRecords, boolean isLastInterval, long roundId) throws Exception {
        log.debug("Downloading CSV file from URL: {} (starting at row: {}, max records: {})", 
                fileUrl, startRow, maxRecords);
        
        try (CloseableHttpResponse response = downloadCsvFile(fileUrl)) {
            return parseCsvToSourceRecords(response.getEntity().getContent(), reportRunId, 
                    intervalStart, intervalEnd, startRow, maxRecords, isLastInterval, roundId);
        }
    }
    
    /**
     * Downloads CSV file from URL using Stripe API key as Bearer token
     * 
     * @param fileUrl The URL of the CSV file
     * @return HTTP response containing the CSV InputStream
     * @throws Exception if download fails
     */
    private CloseableHttpResponse downloadCsvFile(String fileUrl) throws Exception {
        HttpGet request = new HttpGet(fileUrl);
        
        // Set Bearer token authentication
        request.setHeader("Authorization", "Bearer " + apiKey);
        request.setHeader("Accept", "text/csv");
        
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getCode();
        
        if (statusCode == 404) {
            String errorBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            response.close();
            throw new FileNotFoundException("CSV file not found (404). URL: " + fileUrl + ", Response: " + errorBody);
        }
        
        if (statusCode != 200) {
            String errorBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            response.close();
            throw new Exception("Failed to download CSV file. Status: " + statusCode + ", Response: " + errorBody);
        }
        
        return response;
    }
    
    /**
     * Parses CSV from InputStream and converts each row to a SourceRecord using Apache Commons CSV
     * 
     * @param inputStream The CSV InputStream
     * @param reportRunId The report run ID for tracking
     * @param intervalStart The start timestamp of the report interval
     * @param intervalEnd The end timestamp of the report interval
     * @param startRow The row index to start processing from (for resuming)
     * @param maxRecords Maximum number of records to process
     * @return List of SourceRecords
     * @throws Exception if parsing fails
     */
    private List<SourceRecord> parseCsvToSourceRecords(InputStream inputStream, String reportRunId, long intervalStart, long intervalEnd,
                                                      int startRow, int maxRecords, boolean isLastInterval, long roundId) throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        
        if (inputStream == null) {
            log.warn("CSV InputStream is null");
            return records;
        }
        
        try (InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
             CSVParser csvParser = new CSVParser(reader, CSV_FORMAT)) {
            
            // Get headers
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            if (headerMap == null || headerMap.isEmpty()) {
                log.warn("No headers found in CSV");
                return records;
            }
            
            List<String> headers = new ArrayList<>(headerMap.keySet());
            
            // Process each record
            int rowIndex = 0;
            int processedCount = 0;
            boolean reachedEnd = false;
            boolean isReportComplete = true;
            
            for (CSVRecord record : csvParser) {
                // Skip rows before startRow
                if (rowIndex < startRow) {
                    rowIndex++;
                    continue;
                }
                
                // Stop if we've processed maxRecords
                if (processedCount >= maxRecords) {
                    log.debug("Reached max records limit ({}), stopping at row {}", maxRecords, rowIndex);
                    isReportComplete = false;
                    break;
                }
                
                try {
                    // Create JSON object from CSV record
                    Map<String, Object> jsonObject = createJsonFromRecord(record, headers);
                    
                    processedCount++;
                    rowIndex++;
                    
                    // Create record with in-progress state (will be updated for last record if report is complete)
                    SourceRecord sourceRecord = createSourceRecord(jsonObject, reportRunId, rowIndex - 1, 
                            intervalStart, intervalEnd, roundId);
                    records.add(sourceRecord);
                    
                } catch (Exception e) {
                    log.error("Error processing CSV row {}: {}", rowIndex, e.getMessage(), e);
                    // Continue processing other rows
                    rowIndex++;
                }
            }
            
            // Check if we reached end of file (report is complete)
            reachedEnd = isReportComplete && isLastInterval;
            
            // update the last record's offset
            if (!records.isEmpty()) {
                SourceRecord lastRecord = records.get(records.size() - 1);
                SourceRecord updatedLastRecord = updateLastRecordOffset(lastRecord, reachedEnd, isReportComplete, roundId);
                records.set(records.size() - 1, updatedLastRecord);
            }
        }
        return records;
    }
    
    /**
     * Creates a JSON object from a CSV record
     * 
     * @param record The CSV record
     * @param headers List of header names
     * @return Map representing JSON object
     */
    private Map<String, Object> createJsonFromRecord(CSVRecord record, List<String> headers) {
        Map<String, Object> jsonObject = new HashMap<>();
        
        // Add all fields from CSV
        for (String header : headers) {
            String value = record.get(header);
            
            // Try to parse as number if possible
            Object parsedValue = parseValue(value);
            // Replace square brackets with underscores
            header = header.replace("[", "_");
            header = header.replace("]", "_");
            jsonObject.put(header, parsedValue);
        }
        
        // Add metadata
        jsonObject.put("_ingestion_timestamp", System.currentTimeMillis());
        jsonObject.put("_source", "confluent_stripe_reporting");
        
        // Add timezone_aka_platform from config
        if (timezone != null && !timezone.isEmpty()) {
            jsonObject.put("timezone_aka_platform", timezone);
        }

        // Add extract_from_stripe_datetime
        jsonObject.put("extract_from_stripe_datetime", customFormatter.format(Instant.now().atZone(ZoneId.of("UTC"))));
        
        // For payout reconciliation, we need to add an extra field
        if (reportType.equals("payout_reconciliation.itemized.5")) {
            Matcher matcher = datePattern.matcher(record.get("description"));
            if (matcher.find()) {
                String dateParsedFromDescription = matcher.group(1) + "-" + matcher.group(2) + "-01";
                jsonObject.put("year_month", dateParsedFromDescription);
            } else {
                // if no date found in description, try to parse from created-utc
                matcher = datePattern.matcher(record.get("created_utc"));
                if (matcher.find()) {
                    String dateParsedFromCreatedUtc = matcher.group(1) + "-" + matcher.group(2) + "-01";
                    jsonObject.put("year_month", dateParsedFromCreatedUtc);
                } else {
                    jsonObject.put("year_month", DateTimeFormatter.ofPattern("yyyy-MM-01").format(Instant.now().atZone(ZoneId.of("UTC"))));
                }
            }
        }
        return jsonObject;
    }
    
    /**
     * Attempts to parse a string value as a number, otherwise returns as string
     * 
     * @param value The string value
     * @return Parsed value (Number or String)
     */
    private Object parseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        
        // Try to parse as Long
        try {
            // Check if it's a valid long (integer)
            if (!value.contains(".") && !value.contains("e") && !value.contains("E")) {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Not a long, continue
        }
        
        // Try to parse as Double
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            // Not a number, return as string
            return value;
        }
    }
    
    /**
     * Gets the sanitized topic name for the report type
     * 
     * @return Topic name with dots replaced by underscores
     */
    private String getTopicName() {
        String sanitizedReportType = reportType != null ? reportType.replace(".", "_") : "unknown";
        return topicPrefix + sanitizedReportType;
    }

    private SourceRecord updateLastRecordOffset(SourceRecord lastRecord, boolean reachedEnd, boolean isReportComplete, long roundId) throws Exception {
        StripeSourceOffset offset = StripeSourceOffset.fromMap((Map<String, Object>) lastRecord.sourceOffset());
        if (offset == null) {
            return lastRecord;
        }
        if (isReportComplete) {
            offset.setInProgressReportRunId(null);
            offset.setLastCommittedRow(null);
            offset.setIntervalStart(offset.getIntervalEnd());
        } 
        offset.setRoundId(roundId);
        offset.setProcessedInRound(reachedEnd);
        SourceRecord updatedLastRecord = new SourceRecord(lastRecord.sourcePartition(), offset.toMap(), lastRecord.topic(), lastRecord.kafkaPartition(), lastRecord.keySchema(), lastRecord.key(), lastRecord.valueSchema(), lastRecord.value());
        return updatedLastRecord;
    }
    
    /**
     * Creates a SourceRecord from a JSON object
     * 
     * @param jsonObject The JSON object (as Map)
     * @param reportRunId The report run ID
     * @param rowIndex The row index for offset tracking
     * @param intervalStart The start timestamp of the report interval
     * @param intervalEnd The end timestamp of the report interval
     * @param isReportComplete Whether the entire report is complete after this batch
     * @return SourceRecord ready to be sent to Kafka
     * @throws Exception if creation fails
     */
    private SourceRecord createSourceRecord(Map<String, Object> jsonObject, String reportRunId, int rowIndex, 
                                           long intervalStart, long intervalEnd, long roundId) throws Exception {
        try {
            StripeSourcePartition sourcePartition = new StripeSourcePartition(reportType, timezone);
            // Create source offset
            // Kafka Connect commits the offset of the LAST record in a batch after all records are successfully written.
            // Therefore:
            // - For all records: Keep in-progress state by default (for resumption if batch fails)
            // - For the last record when report is complete: Clear in-progress state (report finished)
            StripeSourceOffset sourceOffset;
            // Report in progress - track current state for resumption
            sourceOffset = new StripeSourceOffset(intervalStart, intervalEnd, reportRunId, rowIndex);
            sourceOffset.setRoundId(roundId);
            sourceOffset.setProcessedInRound(Boolean.FALSE);
            
            // Create key from configured key columns or fallback to report_run_id + row_index
            String key;
            if (!keyColumns.isEmpty()) {
                // Use configured key columns from CSV data
                Map<String, Object> keyMap = new HashMap<>();
                for (String keyColumn : keyColumns) {
                    // Try original name first, then normalized (brackets replaced with underscores)
                    Object value = jsonObject.get(keyColumn);
                    if (value == null) {
                        String normalizedKeyColumn = keyColumn.replace("[", "_").replace("]", "_");
                        value = jsonObject.get(normalizedKeyColumn);
                    }
                    if (value != null) {
                        keyMap.put(keyColumn, value);
                    }
                }
                // If no key columns found in data, fallback to default
                if (keyMap.isEmpty()) {
                    keyMap.put("report_run_id", reportRunId);
                    keyMap.put("row_index", rowIndex);
                }
                key = objectMapper.writeValueAsString(keyMap);
            } else {
                // Default: use report run ID + row index as unique identifier
                Map<String, Object> keyMap = new HashMap<>();
                keyMap.put("report_run_id", reportRunId);
                keyMap.put("row_index", rowIndex);
                key = objectMapper.writeValueAsString(keyMap);
            }
            
            // Value is the JSON object
            return new SourceRecord(
                    sourcePartition.toMap(),
                    sourceOffset.toMap(),
                    getTopicName(),
                    null, // Key schema (schemaless)
                    key,
                    null, // Value schema (schemaless)
                    jsonObject
            );
        } catch (Exception e) {
            throw new Exception("Failed to create SourceRecord for row " + rowIndex + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Closes the HTTP client
     */
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (Exception e) {
            log.error("Error closing HTTP client", e);
        }
    }
}
