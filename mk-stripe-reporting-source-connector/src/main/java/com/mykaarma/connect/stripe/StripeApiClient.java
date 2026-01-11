package com.mykaarma.connect.stripe;

import com.stripe.exception.*;
import com.stripe.model.reporting.ReportRun;
import com.stripe.model.reporting.ReportType;
import com.stripe.param.reporting.ReportRunCreateParams;
import com.stripe.param.reporting.ReportRunCreateParams.Parameters;
import com.stripe.param.reporting.ReportRunListParams;
import com.stripe.model.File;
import com.stripe.net.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * HTTP client for interacting with Stripe API using Stripe Java SDK
 */
public class StripeApiClient {
    
    private static final Logger log = LoggerFactory.getLogger(StripeApiClient.class);
    
    private final String apiKey;
    private final long pollIntervalMs;
    private final long maxWaitMs;
    private final String timezone;
    private final StripeSourceConnectorConfig config;
    
    public StripeApiClient(String timezone, StripeSourceConnectorConfig config) throws StripeException {
        this.timezone = timezone;
        this.config = config;
        this.apiKey = config.getTimezoneApiKeyMapping().get(timezone);
        if (this.apiKey == null || this.apiKey.isEmpty()) {
            throw new StripeException("No API key found for timezone: " + timezone);
        }
        this.pollIntervalMs = config.getReportPollIntervalMs();
        this.maxWaitMs = config.getReportMaxWaitMs();
        
        // Test connection with this API key
        if (this.testConnection()) {
            log.info("Stripe API client initialized successfully for timezone: {}", timezone);
        } else {
            throw new StripeException("Stripe API client initialization failed for timezone: " + timezone);
        }
    }
    
    /**
     * Creates RequestOptions with the API key for this timezone.
     * This allows each API call to use the correct API key without relying on static fields.
     * 
     * @return RequestOptions configured with this client's API key
     */
    private RequestOptions getRequestOptions() {
        return RequestOptions.builder()
                .setApiKey(this.apiKey)
                .build();
    }
    
    /**
     * Handles Stripe API exceptions with consistent logging and error messages
     * 
     * @param operation Description of the operation (e.g., "creating report run", "retrieving report run")
     * @param identifier Identifier for the operation (e.g., reportRunId, reportType)
     * @param e The exception that occurred
     * @return StripeException with appropriate error message
     */
    private StripeException handleStripeException(String operation, String identifier, Exception e) {
        if (e instanceof RateLimitException) {
            log.error("Rate limit exceeded while {}: {}", operation, identifier, e);
            return new StripeException("Stripe API rate limit exceeded: " + e.getMessage());
        } else {
            log.error("Exception while {} {}: {}", operation, identifier, e.getMessage(), e);
            return new StripeException("Exception while " + operation + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates a new report run with the given parameters
     * 
     * @param reportType The ID of the report type to run (e.g., "balance.summary.1")
     * @param intervalStart Start timestamp for the report interval
     * @param intervalEnd End timestamp for the report interval
     * @return ReportRunResult containing the report run ID and initial status
     * @throws StripeException if API request fails
     */
    public ReportRunResult createReportRun(String reportType, long intervalStart, long intervalEnd) 
            throws StripeException {
        List<String> columns = config.getColumns(reportType);
        try {
            log.debug("Creating report run: type={}, intervalStart={}, intervalEnd={}, timezone={}", 
                    reportType, intervalStart, intervalEnd, timezone);
            
            // Build parameters
            Parameters parameters = Parameters.builder()
                    .setIntervalStart(intervalStart)
                    .setIntervalEnd(intervalEnd)
                    .addAllColumn(columns)
                    .build();
            
            // Build report run create params
            ReportRunCreateParams reportRunCreateParams = ReportRunCreateParams.builder()
                    .setReportType(reportType)
                    .setParameters(parameters)
                    .build();
            
            // Use RequestOptions to pass API key per request (non-static approach)
            ReportRun reportRun = ReportRun.create(reportRunCreateParams, getRequestOptions());
            
            log.info("Report run created: id={}, status={}", reportRun.getId(), reportRun.getStatus());
            
            return new ReportRunResult(
                    reportRun.getId(),
                    extractFileUrl(reportRun),
                    reportRun.getStatus()
            );
        } catch (Exception e) {
            throw handleStripeException("creating report run", reportType, e);
        }
    }
    
    /**
     * Waits for a report run to complete and returns the file reference
     * 
     * @param reportRunId The ID of the report run to wait for
     * @return ReportRunResult containing the report run ID and file reference when complete
     * @throws StripeException if API request fails or report run fails
     * @throws InterruptedException if polling is interrupted
     */
    public ReportRunResult waitForReportCompletion(String reportRunId) 
            throws StripeException, InterruptedException {
        long startTime = System.currentTimeMillis();
        
        log.debug("Waiting for report run completion: id={}, maxWaitMs={}", reportRunId, maxWaitMs);
        
        while (true) {
            // Check timeout
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= maxWaitMs) {
                throw new StripeException(
                        String.format("Report run %s did not complete within %d ms", reportRunId, maxWaitMs)
                );
            }
            
            ReportRun reportRun = getReportRun(reportRunId);
            String status = reportRun.getStatus();
            
            log.debug("Report run {} status: {}", reportRunId, status);
            
            if ("succeeded".equals(status)) {
                String fileUrl = extractFileUrl(reportRun);
                if (fileUrl == null) {
                    log.warn("Report run {} succeeded but no file URL found", reportRunId);
                }
                
                log.debug("Report run {} completed successfully. File URL: {}", reportRunId, fileUrl);
                return new ReportRunResult(reportRunId, fileUrl, status);
            } else if ("failed".equals(status)) {
                String error = reportRun.getError();
                String errorMsg = String.format("Report run %s failed: %s", reportRunId, error != null ? error : "Unknown error");
                log.error(errorMsg);
                throw new StripeException(errorMsg);
            } else if ("pending".equals(status)) {
                // Continue polling
                long remainingWait = maxWaitMs - elapsed;
                long sleepTime = Math.min(pollIntervalMs, remainingWait);
                
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                } else {
                    throw new StripeException(
                            String.format("Report run %s still pending after %d ms", reportRunId, maxWaitMs)
                    );
                }
            } else {
                log.warn("Unknown report run status: {} for report run {}", status, reportRunId);
                // Continue polling for unknown status
                Thread.sleep(pollIntervalMs);
            }
        }
    }
    
    /**
     * Retrieves a report run by ID
     * 
     * @param reportRunId The ID of the report run to retrieve
     * @return ReportRun object
     * @throws StripeException if API request fails
     */
    public ReportRun getReportRun(String reportRunId) throws StripeException {
        try {
            // Use RequestOptions to pass API key per request (non-static approach)
            return ReportRun.retrieve(reportRunId, getRequestOptions());
        } catch (Exception e) {
            throw handleStripeException("retrieving report run", reportRunId, e);
        }
    }
    
    /**
     * Extracts the file URL from a ReportRun's result field
     * 
     * @param reportRun The ReportRun object
     * @return File URL if available, null otherwise
     */
    private String extractFileUrl(ReportRun reportRun) {
        if (reportRun.getResult() == null) {
            return null;
        }
        
        try {
            File result = reportRun.getResult();
            log.debug("Report run result: title={}, URL={}, size={}, type={}, createdAt={}, expiresAt={}", 
                result.getTitle(), result.getUrl(), result.getSize(), result.getType(), 
                result.getCreated(), result.getExpiresAt());
            return result.getUrl();
        } catch (Exception e) {
            log.warn("Error extracting file URL from report run result", e);
            return null;
        }
    }
    
    /**
     * Retrieves the data_available_end timestamp for a specific report type
     * 
     * @param reportType The ID of the report type (e.g., "balance.summary.1")
     * @return The data_available_end timestamp (Unix timestamp in seconds), or null if not available
     * @throws StripeException if API request fails
     */
    public Long getDataAvailableEnd(String reportType) throws StripeException {
        try {
            log.debug("Retrieving data_available_end for report type: {}, timezone: {}", reportType, timezone);
            
            // Use RequestOptions to pass API key per request (non-static approach)
            ReportType reportTypeObj = ReportType.retrieve(reportType, getRequestOptions());
            
            Long dataAvailableEnd = reportTypeObj.getDataAvailableEnd();
            
            if (dataAvailableEnd != null) {
                log.debug("Report type {} has data_available_end: {}", reportType, dataAvailableEnd);
            } else {
                log.warn("Report type {} does not have data_available_end set", reportType);
                throw new Exception("Report type " + reportType + " does not have data_available_end set");
            }
            
            return dataAvailableEnd;
            
        } catch (Exception e) {
            throw handleStripeException("retrieving report type", reportType, e);
        }
    }
    
    /**
     * Test the API connection
     * 
     * @return true if connection is successful
     */
    public boolean testConnection() {
        try {
            // Use RequestOptions to pass API key per request (non-static approach)
            // Try to list report runs with limit 1 to test connection
            ReportRun.list(ReportRunListParams.builder()
                    .setLimit(1L)
                    .build(), getRequestOptions());
            log.info("Stripe API connection test successful for timezone: {}", timezone);
            return true;
        } catch (RateLimitException e) {
            // Rate limit is still a successful connection
            log.warn("Rate limit hit during connection test, but connection is working");
            return true;
        } catch (AuthenticationException e) {
            log.error("Authentication failed during connection test", e);
            return false;
        } catch (Exception e) {
            log.error("Exception while testing connection: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Generates SourceRecords from a Stripe report run for a specific report type
     * Orchestrates the full flow: create report → wait → get file URL → process CSV → return SourceRecords
     * 
     * @param reportType The report type to process
     * @param intervalStart Start timestamp for the report interval
     * @param intervalEnd End timestamp for the report interval
     * @param reportRunId Existing report run ID to resume (null to create new)
     * @param startRow Row index to start from (0 for new report)
     * @param maxRecords Maximum number of records to process in this batch
     * @return List of SourceRecords ready to be sent to Kafka
     * @throws StripeException if API request fails or report run fails
     * @throws InterruptedException if polling is interrupted
     * @throws Exception if CSV download or parsing fails
     */
    public List<SourceRecord> generateSourceRecordsForSingleReportType(String reportType, long intervalStart, long intervalEnd, 
                                                    String reportRunId, int startRow, int maxRecords, boolean isLastInterval, long roundId) 
            throws StripeException, InterruptedException, Exception {
        log.debug("Generating SourceRecords for report type: {}, interval: {} to {} (startRow: {}, maxRecords: {})", 
                reportType, intervalStart, intervalEnd, startRow, maxRecords);
        // List<String> columns = config.getColumns(reportType);
        List<String> keyColumns = config.getKeys(reportType);
        String topicPrefix = config.getTopicPrefix();
        String fileUrl = null;
        // Step 1: Use existing report or create new one
        if (reportRunId == null) {
            ReportRunResult reportRunResult = createReportRun(reportType, intervalStart, intervalEnd);
            reportRunId = reportRunResult.getReportRunId();
            reportRunResult = waitForReportCompletion(reportRunId);
            fileUrl = reportRunResult.getFileUrl();
            
            if (fileUrl == null || fileUrl.isEmpty()) {
                throw new StripeException("Report run completed but file URL is null or empty");
            }
        } else {
            log.debug("Resuming existing report run: {} from row {}", reportRunId, startRow);
            fileUrl = extractFileUrl(getReportRun(reportRunId));
        }
        
        // Step 2: Process CSV file and generate SourceRecords (with batching)
        StripeCsvProcessor csvProcessor = new StripeCsvProcessor(apiKey, topicPrefix, reportType, timezone, keyColumns);
        try {
            List<SourceRecord> records = csvProcessor.processCsvFile(fileUrl, reportRunId, intervalStart, intervalEnd, 
                startRow, maxRecords, isLastInterval, roundId);
            log.info("Generated {} SourceRecords for report type: {} and timezone: {} (report run: {})", 
                    records.size(), reportType, timezone, reportRunId);
            return records;
        } catch (java.io.FileNotFoundException e) {
            log.warn("CSV file not found (404) for report run {}, creating new report run and retrying: {}", 
                    reportRunId, e.getMessage());
            
            ReportRunResult reportRunResult = createReportRun(reportType, intervalStart, intervalEnd);
            String newReportRunId = reportRunResult.getReportRunId();
            reportRunResult = waitForReportCompletion(newReportRunId);
            String newFileUrl = reportRunResult.getFileUrl();
            
            log.info("Retrying CSV download with new report run: {} and file URL: {}", newReportRunId, newFileUrl);
            
            List<SourceRecord> records = csvProcessor.processCsvFile(newFileUrl, newReportRunId, intervalStart, intervalEnd, 
                    startRow, maxRecords, isLastInterval, roundId);
            log.info("Generated {} SourceRecords for report type: {} (new report run: {})", 
                    records.size(), reportType, newReportRunId);
            return records;
        } catch (Exception e) {
            log.error("Error generating SourceRecords for report type: {}", reportType, e);
            throw new StripeException("Error generating SourceRecords for report type: " + reportType + ": " + e.getMessage(), e);
        } finally {
            csvProcessor.close();
        }
    }
    
    public void close() {
        // Stripe SDK doesn't require explicit cleanup
        log.debug("Stripe API client closed");
    }
}