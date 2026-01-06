package com.mykaarma.connect.chargeover;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * SourceTask implementation for ChargeOver connector with cron-based scheduling and continuous pagination
 */
public class ChargeOverSourceTask extends SourceTask {
    
    private static final Logger log = LoggerFactory.getLogger(ChargeOverSourceTask.class);
    
    // Load modes
    private enum LoadMode {
        INITIAL_LOAD,
        INCREMENTAL_LOAD
    }
    
    // Entity state tracker
    private static class EntityState {
        LoadMode loadMode;
        String lastProcessedDatetime; // YYYY-MM-DD HH:MM:SS format
        String batchEndDatetime; // End datetime for current batch (upper bound)
        long nextScheduledRun; // Timestamp for next scheduled incremental run
        int currentOffset; // Current pagination offset within a batch
        boolean isProcessingBatch; // Flag to indicate if currently processing a batch
        int retryCount;
        
        EntityState() {
            this.loadMode = LoadMode.INITIAL_LOAD;
            this.lastProcessedDatetime = null;
            this.batchEndDatetime = null;
            this.nextScheduledRun = 0L;
            this.currentOffset = 0;
            this.isProcessingBatch = false;
            this.retryCount = 0;
        }
    }
    
    private ChargeOverSourceConnectorConfig config;
    private ChargeOverApiClient apiClient;
    private ObjectMapper objectMapper;
    private long lastPollTime;
    private Map<String, EntityState> entityStateMap;
    private SimpleDateFormat dateFormat;
    
    @Override
    public String version() {
        return "1.0.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting ChargeOver Source Task");
        
        config = new ChargeOverSourceConnectorConfig(props);
        apiClient = new ChargeOverApiClient(
            config.getApiUrl(),
            config.getUsername(),
            config.getPassword()
        );
        objectMapper = new ObjectMapper();
        lastPollTime = System.currentTimeMillis();
        entityStateMap = new HashMap<>();
        
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone(config.getTimezone()));
        
        // Initialize entity states from offset storage
        for (String entity : config.getEntities()) {
            EntityState state = loadEntityState(entity);
            entityStateMap.put(entity, state);
            
            log.info("Initialized entity: {} in mode: {} with lastProcessedDatetime: {}, nextScheduledRun: {}", 
                    entity, state.loadMode, state.lastProcessedDatetime, formatTimestamp(state.nextScheduledRun));
        }
        
        log.info("Task started for entities: {}", config.getEntities());
    }
    
    /**
     * Load entity state from offset storage or initialize new state
     */
    private EntityState loadEntityState(String entity) {
        Map<String, Object> offset = context.offsetStorageReader()
            .offset(Collections.singletonMap("entity", entity));
        
        EntityState state = new EntityState();
        
        if (offset != null && !offset.isEmpty()) {
            // Resume from stored offset
            state.loadMode = LoadMode.valueOf((String) offset.getOrDefault("load_mode", "INITIAL_LOAD"));
            state.lastProcessedDatetime = (String) offset.get("last_processed_datetime");
            state.batchEndDatetime = (String) offset.get("batch_end_datetime");
            state.nextScheduledRun = (Long) offset.getOrDefault("next_scheduled_run", 0L);
            state.currentOffset = ((Number) offset.getOrDefault("current_offset", 0)).intValue();
            state.isProcessingBatch = (Boolean) offset.getOrDefault("is_processing_batch", false);
            state.retryCount = ((Number) offset.getOrDefault("retry_count", 0)).intValue();
            
            log.info("Resuming entity: {} from offset - mode: {}, lastProcessedDatetime: {}, offset: {}", 
                    entity, state.loadMode, state.lastProcessedDatetime, state.currentOffset);
        } else {
            // Initialize new state
            String initialDatetime = config.getInitialDatetimeForEntity(entity);
            if (initialDatetime != null) {
                state.loadMode = LoadMode.INITIAL_LOAD;
                state.lastProcessedDatetime = initialDatetime;
                log.info("Starting fresh initial load for entity: {} from {}", entity, initialDatetime);
            } else {
                // No initial datetime configured, start from current datetime
                state.loadMode = LoadMode.INITIAL_LOAD;
                state.lastProcessedDatetime = getCurrentDatetime();
                log.info("No initial datetime configured, starting for entity: {} from current datetime {}", 
                        entity, state.lastProcessedDatetime);
            }
        }
        
        return state;
    }
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long currentTime = System.currentTimeMillis();
        
        // Check if any entity is actively processing a batch
        boolean isAnyBatchInProgress = entityStateMap.values().stream()
            .anyMatch(state -> state.isProcessingBatch);
        
        // Respect poll interval only when not processing a batch
        if (!isAnyBatchInProgress && currentTime - lastPollTime < config.getPollIntervalMs()) {
            Thread.sleep(Math.min(config.getPollIntervalMs() - (currentTime - lastPollTime), 1000));
            return null;
        }
        
        lastPollTime = System.currentTimeMillis();
        
        List<SourceRecord> records = new ArrayList<>();
        
        // Process each entity
        for (String entity : config.getEntities()) {
            try {
                EntityState state = entityStateMap.get(entity);
                
                // Check if this entity is ready for processing
                if (!isReadyForProcessing(state)) {
                    continue;
                }
                
                // Process the entity (fetches one page at a time)
                List<SourceRecord> entityRecords = processEntity(entity, state);
                records.addAll(entityRecords);
                
            } catch (Exception e) {
                log.error("Error processing entity: {}", entity, e);
            }
        }
        
        return records.isEmpty() ? null : records;
    }
    
    /**
     * Check if an entity is ready for processing
     */
    private boolean isReadyForProcessing(EntityState state) {
        if (state.loadMode == LoadMode.INITIAL_LOAD) {
            // Initial load: always ready if not caught up to current datetime
            String currentDatetime = getCurrentDatetime();
            return state.lastProcessedDatetime == null || 
                   state.lastProcessedDatetime.compareTo(currentDatetime) < 0 ||
                   state.isProcessingBatch;
        } else {
            // Incremental load: ready if it's time for next scheduled run or currently processing
            long currentTime = System.currentTimeMillis();
            return currentTime >= state.nextScheduledRun || state.isProcessingBatch;
        }
    }
    
    /**
     * Process an entity - fetches one page at a time
     */
    private List<SourceRecord> processEntity(String entity, EntityState state) {
        List<SourceRecord> records = new ArrayList<>();
        
        try {
            // If not already processing, start a new batch
            if (!state.isProcessingBatch) {
                startNewBatch(entity, state);
            }
            
            // Fetch one page for this batch
            String datetimeField = config.getDatetimeFieldForEntity(entity);
            String queryParams = config.getQueryParamsForEntity(entity);
            
            ChargeOverApiClient.FetchResult result = fetchBatchWithRetry(
                    entity, datetimeField, state.lastProcessedDatetime, state.batchEndDatetime, state.currentOffset, queryParams
            );
            
            // Process records
            for (JsonNode record : result.getRecords()) {
                SourceRecord sourceRecord = createSourceRecord(entity, record, state);
                if (sourceRecord != null) {
                    records.add(sourceRecord);
                }
            }
            
            // Update pagination state
            if (result.hasMore()) {
                // More records available, advance offset
                state.currentOffset += result.getTotalFetched();
                state.retryCount = 0; // Reset retry count on success
                log.debug("Entity: {} - fetched {} records at offset {}, more available", 
                         entity, result.getTotalFetched(), state.currentOffset);
            } else {
                // All pages fetched, complete the batch
                completeBatch(entity, state);
            }
            
            // Save state after each page
            saveEntityState(entity, state);
            
        } catch (Exception e) {
            handleFetchError(entity, state, e);
        }
        
        return records;
    }
    
    /**
     * Start a new batch for an entity
     */
    private void startNewBatch(String entity, EntityState state) {
        // Capture current datetime as the upper bound for this batch
        String currentDatetime = getCurrentDatetime();
        state.batchEndDatetime = currentDatetime;
        
        if (state.loadMode == LoadMode.INITIAL_LOAD) {
            // Initial load: process from last processed datetime to current datetime
            log.info("Entity: {} - starting initial load from {} to {}", 
                    entity, state.lastProcessedDatetime, currentDatetime);
        } else {
            // Incremental load: process from last processed datetime to current datetime
            log.info("Entity: {} - starting incremental load from {} to {}", 
                    entity, state.lastProcessedDatetime, currentDatetime);
        }
        
        state.isProcessingBatch = true;
        state.currentOffset = 0;
    }
    
    /**
     * Complete a batch after all pages are fetched
     */
    private void completeBatch(String entity, EntityState state) {
        log.info("Entity: {} - completed batch, processed up to {}", entity, state.batchEndDatetime);
        
        // Update last processed datetime to the captured batch end datetime
        state.lastProcessedDatetime = state.batchEndDatetime;
        
        // Check if we should switch from initial to incremental mode
        if (state.loadMode == LoadMode.INITIAL_LOAD) {
            log.info("Initial load complete for entity: {}. Switching to incremental mode.", entity);
            state.loadMode = LoadMode.INCREMENTAL_LOAD;
        }
        
        // Calculate next scheduled run after batch completion
        // (This happens after BOTH initial and incremental loads)
        state.nextScheduledRun = calculateNextScheduledRun();
        log.info("Entity: {} - next scheduled run at {}", entity, formatTimestamp(state.nextScheduledRun));
        
        // Clean up batch state
        state.batchEndDatetime = null;
        state.isProcessingBatch = false;
        state.currentOffset = 0;
        state.retryCount = 0;
        
        saveEntityState(entity, state);
    }
    
    /**
     * Fetch batch with retry logic using exponential backoff with jitter
     */
    private ChargeOverApiClient.FetchResult fetchBatchWithRetry(
            String entity, String datetimeField, String startDatetime, String endDatetime, int offset, String additionalQueryParams) throws Exception {
        
        Exception lastException = null;
        
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                ChargeOverApiClient.FetchResult result = apiClient.fetchChangesWithPagination(
                    entity, datetimeField, startDatetime, endDatetime, offset, config.getBatchSize(), additionalQueryParams
                );
                
                // Success
                if (attempt > 0) {
                    log.info("Entity: {} - retry succeeded on attempt {}", entity, attempt + 1);
                }
                
                return result;
                
            } catch (ChargeOverRateLimitException e) {
                lastException = e;
                log.warn("Entity: {} - rate limit hit on attempt {} of {}", 
                        entity, attempt + 1, config.getMaxRetries() + 1);
                
                if (attempt < config.getMaxRetries()) {
                    // Wait 1 minute before retrying rate limit errors
                    long backoffMs = 60000; // 1 minute
                    log.info("Entity: {} - backing off for {} seconds due to rate limit", entity, backoffMs / 1000);
                    Thread.sleep(backoffMs);
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("Entity: {} - fetch attempt {} failed: {}", entity, attempt + 1, e.getMessage());
                
                if (attempt < config.getMaxRetries()) {
                    // Exponential backoff with jitter for general errors
                    // Formula: base * 2^attempt + random jitter (0-10% of base)
                    // Example progression: ~1s, ~2s, ~4s, ~8s, ~16s (capped at 30s)
                    long baseBackoffMs = (long) Math.pow(2, attempt) * 1000;
                    long jitterMs = (long) (baseBackoffMs * 0.1 * Math.random());
                    long backoffMs = Math.min(baseBackoffMs + jitterMs, 30000); // Max 30 seconds
                    Thread.sleep(backoffMs);
                }
            }
        }
        
        // All retries exhausted
        throw new Exception("Failed after " + (config.getMaxRetries() + 1) + " attempts", lastException);
    }
    
    /**
     * Handle fetch error after all retries exhausted
     */
    private void handleFetchError(String entity, EntityState state, Exception e) {
        log.error("Entity: {} - failed to fetch data from {} at offset {} after {} retries. Will retry on next poll.", 
                 entity, state.lastProcessedDatetime, state.currentOffset, config.getMaxRetries(), e);
        
        // Increment retry count but keep state for retry on next poll
        state.retryCount++;
        
        // If too many consecutive failures, skip to next scheduled run
        if (state.retryCount > 10) {
            log.error("Entity: {} - too many consecutive failures, resetting batch", entity);
            state.isProcessingBatch = false;
        state.currentOffset = 0;
        state.retryCount = 0;
            
            if (state.loadMode == LoadMode.INCREMENTAL_LOAD) {
                state.nextScheduledRun = calculateNextScheduledRun();
            }
        }
        
        saveEntityState(entity, state);
    }
    
    /**
     * Get current datetime in YYYY-MM-DD HH:MM:SS format
     */
    private String getCurrentDatetime() {
        return dateFormat.format(new Date());
    }
    
    /**
     * Calculate next scheduled run time based on cron expression
     */
    private long calculateNextScheduledRun() {
        try {
            CronExpression cronExpression = config.getIncrementalScheduleCronExpression();
            Date nextRunDate = cronExpression.getNextValidTimeAfter(new Date());
            return nextRunDate.getTime();
        } catch (Exception e) {
            log.error("Error calculating next scheduled run, using 24 hours from now", e);
            return System.currentTimeMillis() + 86400000L; // 24 hours
        }
    }
    
    /**
     * Create source record from JSON data
     */
    private SourceRecord createSourceRecord(String entity, JsonNode data, EntityState state) {
        try {
            // Extract ID for the record
            String idFieldName = config.getIdFieldForEntity(entity);
            String id = data.has(idFieldName) ? data.get(idFieldName).asText() : null;
            if (id == null) {
                log.warn("Skipping record without ID field '{}' for entity: {}", idFieldName, entity);
                return null;
            }
            
            // Create source partition
            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("entity", entity);
            
            // Create source offset with full state
            Map<String, Object> sourceOffset = new HashMap<>();
            sourceOffset.put("load_mode", state.loadMode.name());
            sourceOffset.put("last_processed_datetime", state.lastProcessedDatetime);
            sourceOffset.put("batch_end_datetime", state.batchEndDatetime);
            sourceOffset.put("next_scheduled_run", state.nextScheduledRun);
            sourceOffset.put("current_offset", state.currentOffset);
            sourceOffset.put("is_processing_batch", state.isProcessingBatch);
            sourceOffset.put("retry_count", state.retryCount);
            
            // Topic name
            String topic = config.getTopicPrefix() + "." + entity;
            
            // Convert JsonNode to Map
            @SuppressWarnings("unchecked")
            Map<String, Object> value = objectMapper.convertValue(data, Map.class);
            
            // Add metadata
            value.put("_entity_type", entity);
            value.put("_ingestion_timestamp", System.currentTimeMillis());
            value.put("_load_mode", state.loadMode.name());
            
            // Create JSON key structure with actual field name
            Map<String, Object> key = new HashMap<>();
            key.put(idFieldName, id);
            
            return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null,
                null,
                key,
                null,
                value
            );
            
        } catch (Exception e) {
            log.error("Error creating source record for entity: {}", entity, e);
            return null;
        }
    }
    
    /**
     * Save entity state to offset storage
     */
    private void saveEntityState(String entity, EntityState state) {
        // State is automatically saved via sourceOffset in SourceRecord
        // This method is for explicit saves if needed
        log.debug("Entity: {} state - mode: {}, lastProcessedDatetime: {}, offset: {}, nextScheduledRun: {}", 
                 entity, state.loadMode, state.lastProcessedDatetime, state.currentOffset, 
                 formatTimestamp(state.nextScheduledRun));
    }
    
    /**
     * Format timestamp for logging
     */
    private String formatTimestamp(long timestamp) {
        if (timestamp == 0) {
            return "not scheduled";
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone(config.getTimezone()));
        return sdf.format(new Date(timestamp));
    }
    
    @Override
    public void stop() {
        log.info("Stopping ChargeOver Source Task");
        if (apiClient != null) {
            apiClient.close();
        }
    }
}
