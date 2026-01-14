package com.mykaarma.connect.stripe;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.quartz.CronExpression;
import com.cronutils.parser.CronParser;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.model.Cron;
import static com.cronutils.model.CronType.QUARTZ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.util.Optional;


/**
 * SourceTask implementation for Stripe Reporting connector
 */
public class StripeSourceTask extends SourceTask {
    
    private static final Logger log = LoggerFactory.getLogger(StripeSourceTask.class);
    
    private StripeSourceConnectorConfig config;
    private Map<String, StripeApiClient> apiClients; // timezone -> StripeApiClient
    private CronExpression pollCronExpression;
    private CronParser cronParser;
    private Date nextRoundTime;
    private long nextPollTimeEpochMillis;
    
    // In-memory state tracking per (timezone, reportType) combination
    // Key: "timezone|reportType", Value: Last processed state
    // Used for immediate duplicate prevention while persisted offsets serve as source of truth for recovery
    private final Map<String, StripeReportState> inMemoryState = new ConcurrentHashMap<>();


    /**
     * Represents a (timezone, reportType) combination for processing
     */
    public static class ProcessingCombination {
        final String timezone;
        final String reportType;
        
        ProcessingCombination(String timezone, String reportType) {
            this.timezone = timezone;
            this.reportType = reportType;
        }
    }
    
    /**
     * Generates a key for a (timezone, reportType) combination
     */
    private String getCombinationKey(String timezone, String reportType) {
        return timezone + "|" + reportType;
    }
    
    /**
     * Gets the report state for a combination, checking in-memory state first,
     * then falling back to persisted offset storage and converting it to StripeReportState.
     * 
     * @param timezone The timezone
     * @param reportType The report type
     * @return StripeReportState from in-memory or converted from persisted offset, or null if neither exists
     */
    private StripeReportState getOffsetInfo(String timezone, String reportType) {
        String comboKey = getCombinationKey(timezone, reportType);
        
        // Get in-memory state first
        StripeReportState inMemoryState = this.inMemoryState.get(comboKey);
        if (inMemoryState != null) {
            return inMemoryState;
        }
        
        // Fall back to persisted offset and convert to StripeReportState
        if (context != null) {
            StripeSourcePartition sourcePartition = new StripeSourcePartition(reportType, timezone);
            Map<String, Object> offsetMap = context.offsetStorageReader().offset(sourcePartition.toMap());
            StripeSourceOffset persistedOffset = StripeSourceOffset.fromMap(offsetMap);
            if (persistedOffset != null) {
                log.info("Getting persisted offset for combination: {}", comboKey);
                return StripeReportState.fromOffset(persistedOffset);
            }
        }
        return null;
    }

    
    @Override
    public String version() {
        return "1.0.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Stripe Reporting Source Task");
        
        config = new StripeSourceConnectorConfig(props);
        apiClients = new HashMap<>();
        cronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        
        // Parse and validate cron expression
        String cronExpression = config.getPollCron();
        try {
            pollCronExpression = new CronExpression(cronExpression);
            nextRoundTime = pollCronExpression.getNextValidTimeAfter(new Date());
            log.info("Poll cron expression configured: {}, next poll scheduled at: {}", 
                    cronExpression, nextRoundTime);
        } catch (ParseException e) {
            log.error("Invalid cron expression '{}': {}", cronExpression, e.getMessage());
            throw new RuntimeException("Invalid cron expression: " + cronExpression, e);
        }
        
        // Create an API client for each timezone/API key pair
        Map<String, String> timezoneApiKeyMapping = config.getTimezoneApiKeyMapping();
        for (Map.Entry<String, String> entry : timezoneApiKeyMapping.entrySet()) {
            String timezone = entry.getKey();
            try {
                StripeApiClient client = new StripeApiClient(timezone, config);
                apiClients.put(timezone, client);
                log.info("Created API client for timezone: {}", timezone);
            } catch (StripeException e) {
                log.error("Error creating API client for timezone {}: {}", timezone, e.getMessage());
            }
        }
        
        log.info("Task started with report types: {} and timezones: {}", 
                config.getReportTypes(), apiClients.keySet());
    }
    
    /**
     * Gets the existing round ID from cron schedule. It should be the timestamp of last poll time.
     * 
     * @return Round ID of current cron poll
     */
    private Long getExistingPollRoundId() {
        if (pollCronExpression == null) {
            return null;
        }
        Cron cron = cronParser.parse(pollCronExpression.getCronExpression());
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        Optional<ZonedDateTime> lastExecution = executionTime.lastExecution(now);
        return lastExecution.isPresent() ? lastExecution.get().toEpochSecond() : null;
    }
    
    private Long getNextPollRoundId(Long fromPollRoundId) {
        if (pollCronExpression == null) {
            return null;
        }
        Cron cron = cronParser.parse(pollCronExpression.getCronExpression());
        ZonedDateTime fromPollTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(fromPollRoundId), ZoneOffset.UTC);
        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(fromPollTime);
        return nextExecution.isPresent() ? nextExecution.get().toEpochSecond() : null;
    }

    /**
     * Gets the last processed round ID from in-memory state or offsets
     * Checks in-memory state first, then falls back to persisted offsets
     * 
     * @param processingOrder List of ProcessingCombination in sorted order
     * @return Last processed round ID, or null if no round found
     */
    private Long getLastProcessedRoundId(List<ProcessingCombination> processingOrder) {
        if (processingOrder.isEmpty()) {
            return null;
        }
        
        // Check state (from in-memory or persisted offset)
        for (ProcessingCombination combo : processingOrder) {
            StripeReportState state = getOffsetInfo(combo.timezone, combo.reportType);
            
            if (state != null && state.getRoundId() != null) {
                log.info("Found last processed round ID for {}: {}", 
                        combo.timezone + " " + combo.reportType, state.getRoundId());
                return state.getRoundId();
            }
        }
        log.info("No last processed round ID found for any processing combination");
        return null;
    }


    private SourceRecord createEmptyOffsetMarker(ProcessingCombination combo, Long roundId, Long intervalStart,
        Long intervalEnd, String topicPrefix) throws Exception {
        StripeSourcePartition sourcePartition = new StripeSourcePartition(combo.reportType, combo.timezone);

        // Offset marked as complete
        StripeSourceOffset completedOffset = new StripeSourceOffset(
        intervalStart, intervalEnd, null, null, roundId, true);
        
        String topicName = topicPrefix + (combo.reportType != null ? combo.reportType.replace(".", "_") : "unknown");

        return new SourceRecord(
            sourcePartition.toMap(),
            completedOffset.toMap(),
            topicName,
            null, null, null, null
        );
    }

    /**
     * Checks if the round is completed
     * Checks in-memory state first, then falls back to persisted offsets
     * 
     * @param processingOrder List of ProcessingCombination in sorted order
     * @param roundId The round ID to check
     * @return true if the round is completed for all processing combinations, false otherwise
     */
    private boolean isRoundCompleted(List<ProcessingCombination> processingOrder, Long roundId) {
        if (roundId == null) {
            return false;
        }
        
        for (ProcessingCombination combo : processingOrder) {
            StripeReportState state = getOffsetInfo(combo.timezone, combo.reportType);
            
            // Check if state shows completion (intervalStart == intervalEnd and no in-progress report)
            if (state != null && state.isForRound(roundId)) {
                if (state.isCompletedForRound(roundId)) {
                    log.debug("Combination {} is completed in round {}", 
                            combo.timezone + " " + combo.reportType, roundId);
                } else {
                    return false;
                }
            }
        }
        return true;
    }
    /**
     * Generates deterministic processing order for all (timezone, reportType) combinations
     * 
     * @return List of ProcessingCombination in sorted order
     */
    private List<ProcessingCombination> getProcessingOrder() {
        List<String> sortedTimezones = new ArrayList<>(apiClients.keySet());
        Collections.sort(sortedTimezones);
        
        List<String> sortedReportTypes = new ArrayList<>(config.getReportTypes());
        Collections.sort(sortedReportTypes);
        
        List<ProcessingCombination> combinations = new ArrayList<>();
        for (String timezone : sortedTimezones) {
            for (String reportType : sortedReportTypes) {
                combinations.add(new ProcessingCombination(timezone, reportType));
            }
        }
        
        return combinations;
    }

    private List<SourceRecord> getRemainingRecordsForRound(List<ProcessingCombination> processingOrder, Long roundId) throws Exception {
        List<SourceRecord> allRecords = new ArrayList<>();
        for (ProcessingCombination combo : processingOrder) {
            if (allRecords.size() >= config.getBatchSize()) {
                log.info("Reached batch size limit ({}), will resume round {} on next poll", config.getBatchSize(), roundId);
                return allRecords;
            }
            String timezone = combo.timezone;
            String reportType = combo.reportType;
            String comboKey = getCombinationKey(timezone, reportType);
            
            // Get state (from in-memory or persisted offset converted to StripeReportState)
            StripeReportState state = getOffsetInfo(timezone, reportType);
            
            StripeApiClient apiClient = apiClients.get(timezone);
            if (apiClient == null) {
                log.error("No API client found for timezone: {}, skipping", timezone);
                continue;
            }
            
            Long intervalStart = null;
            Long intervalEnd = null;
            String existingReportRunId = null;
            int lastCommittedRow = -1;
            
            // Determine processing parameters based on state
            if (state != null && state.isForRound(roundId)) {
                if (state.isCompletedForRound(roundId)) {
                    log.info("Combination {} is completed in round {}, skipping", 
                            comboKey, roundId);
                    continue;
                }
                log.info("Combination {} is not completed in round {}, resuming processing with state: {}", 
                        comboKey, roundId, state);
                // Resume in-progress processing using state
                intervalStart = state.getIntervalStart();
                intervalEnd = state.getIntervalEnd();
                existingReportRunId = state.getReportRunId();
                lastCommittedRow = state.getLastCommittedRow() != null ? 
                        state.getLastCommittedRow() : -1;
                
                if (state.hasInProgressReport()) {
                    log.info("Resuming in-progress combination {} in round {} with report run {} from row {} (interval: {} to {})", 
                            combo.timezone + " " + combo.reportType, roundId, existingReportRunId, 
                            lastCommittedRow + 1, intervalStart, intervalEnd);
                } else {
                    intervalStart = state.getIntervalEnd();
                    // intervalEnd = apiClient.getDataAvailableEnd(reportType);
                }
            } else {
                // No offset or first time - start fresh
                intervalStart = config.getInitialLoadDate(reportType);
                // intervalEnd = apiClient.getDataAvailableEnd(reportType);
            }
            if (intervalStart == null) {
                log.error("No interval start found for timezone {} report type {}, skipping", timezone, reportType);
                continue;
            }
            if (intervalStart >= intervalEnd) {
                log.debug("No new data available for timezone {} report type {}, skipping", timezone, reportType);
                StripeReportState completedState = new StripeReportState(
                    roundId, null, intervalStart, intervalEnd, true, null);
                this.inMemoryState.put(comboKey, completedState);
                log.info("Marked combination {} as completed in round {} (interval: {} to {})", 
                        comboKey, roundId, intervalStart, intervalEnd);
                continue;
            }
            log.info("Before checking max days: Interval start: {} interval end: {} for timezone {} report type {}", intervalStart, intervalEnd, timezone, reportType);
            boolean isLastInterval = true;
            // temp fix: always set intervalEnd to available end
            intervalEnd = apiClient.getDataAvailableEnd(reportType);
            Long maxDaysInSeconds = config.getMaxReportIntervalDays() * 24L * 60L * 60L;
            if (intervalStart + maxDaysInSeconds < intervalEnd) {
                log.info("Setting isLastInterval to false for timezone {} report type {} with start: {} end: {}", timezone, reportType, intervalStart, intervalEnd);
                intervalEnd = intervalStart + maxDaysInSeconds;
                isLastInterval = false;
            }
            log.info("After checking max days: Interval start: {} interval end: {} for timezone {} report type {}", intervalStart, intervalEnd, timezone, reportType);
            String startTime = new Date(intervalStart * 1000).toString();
            String endTime = new Date(intervalEnd * 1000).toString();
            log.info("Start getting records for round {} report type {} timezone {} start: {} end: {} from row {}", roundId, reportType, timezone, startTime, endTime, lastCommittedRow + 1);
            List<SourceRecord> records = apiClient.generateSourceRecordsForSingleReportType(
                reportType, intervalStart, intervalEnd, existingReportRunId, lastCommittedRow + 1, config.getBatchSize() - allRecords.size(), isLastInterval, roundId);
            log.info("SourceTask got {} records for round {} report type {} timezone {} start: {} end: {} from row {}", records.size(), roundId, reportType, timezone, startTime, endTime, lastCommittedRow + 1);
            allRecords.addAll(records);
            
            // Update in-memory state from the last record
            if (!records.isEmpty()) {
                SourceRecord lastRecord = records.get(records.size() - 1);
                StripeSourceOffset lastOffset = StripeSourceOffset.fromMap(
                        (Map<String, Object>) lastRecord.sourceOffset());
                
                StripeReportState newState = StripeReportState.fromOffset(lastOffset);
                this.inMemoryState.put(comboKey, newState);
                log.info("Updated in-memory state for {}: {}", comboKey, newState);
            } else {
                // No records - interval is complete
                // Mark interval as complete by setting intervalStart = intervalEnd
                // This ensures the next poll will skip this interval (intervalStart >= intervalEnd check)
                StripeReportState completedState = new StripeReportState(
                    roundId, null, intervalStart, intervalEnd, isLastInterval, null);
                this.inMemoryState.put(comboKey, completedState);
                log.info("Marked combination {} as completed in round {} (interval: {} to {})", 
                        comboKey, roundId, intervalStart, intervalEnd);
                allRecords.add(createEmptyOffsetMarker(combo, roundId, intervalStart, intervalEnd, config.getTopicPrefix()));
                log.info("Created empty offset marker for round {} report type {} timezone {}", roundId, reportType, timezone);
            }
        }
        return allRecords;
    }
    
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            // Time check
            long currentTimeMs = System.currentTimeMillis();
            if (currentTimeMs < nextPollTimeEpochMillis) {
                return null;
            }
            
            // Get round ID and processing order
            Long currentRoundId = getExistingPollRoundId();
            log.info("Current round ID: {}", currentRoundId);
            List<ProcessingCombination> processingOrder = getProcessingOrder();
            Long lastProcessedRoundId = getLastProcessedRoundId(processingOrder);
            
            // Determine which round to process
            Boolean doInitialLoad = (lastProcessedRoundId == null);
            boolean lastRoundCompleted = isRoundCompleted(processingOrder, lastProcessedRoundId);
            Date currentTime = new Date();
            
            if (!doInitialLoad && lastRoundCompleted && lastProcessedRoundId == currentRoundId) {
                // Time to poll - update next poll time based on cron expression
                nextRoundTime = pollCronExpression.getNextValidTimeAfter(currentTime);
                nextPollTimeEpochMillis = nextRoundTime.getTime();
                log.info("Polling triggered by cron schedule, next poll scheduled at: {}. Current round ID: {}", 
                        nextRoundTime, currentRoundId);
                return null;
            }
            
            Long finalRoundId = currentRoundId;
            if (doInitialLoad) {
                log.info("Doing initial load, will start from current round ID: {}", currentRoundId);
            } else if (!lastRoundCompleted) {
                finalRoundId = lastProcessedRoundId;
            } else if (lastProcessedRoundId < currentRoundId) {
                finalRoundId = getNextPollRoundId(lastProcessedRoundId);
            }
            
            List<SourceRecord> records = getRemainingRecordsForRound(processingOrder, finalRoundId);
            
            nextPollTimeEpochMillis = System.currentTimeMillis() + config.getInProgressPollIntervalMs();
            return records.isEmpty() ? null : records;
        } catch (InterruptedException e) {
            log.error("Interrupted while polling", e);
            nextPollTimeEpochMillis = System.currentTimeMillis() + config.getInProgressPollIntervalMs();
            return null;
        } catch (Exception e) {
            log.error("Error while polling", e);
            nextPollTimeEpochMillis = System.currentTimeMillis() + config.getInProgressPollIntervalMs();
            return null;
        }
    }
    
    @Override
    public void stop() {
        log.info("Stopping Stripe Reporting Source Task");
        
        // Clear in-memory state on stop
        inMemoryState.clear();
        log.debug("Cleared in-memory state");
        
        if (apiClients != null) {
            for (Map.Entry<String, StripeApiClient> entry : apiClients.entrySet()) {
                String timezone = entry.getKey();
                StripeApiClient client = entry.getValue();
                log.info("Closing API client for timezone: {}", timezone);
                client.close();
            }
            apiClients.clear();
        }
    }
}