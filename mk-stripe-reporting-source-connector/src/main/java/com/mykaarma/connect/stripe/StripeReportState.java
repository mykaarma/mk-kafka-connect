package com.mykaarma.connect.stripe;

import java.util.Objects;

/**
 * Tracks the last processed state for a (timezone, reportType) combination.
 * Used for in-memory duplicate prevention while persisted offsets serve as the source of truth for recovery.
 */
public class StripeReportState {
    private final Long roundId;
    private final String reportRunId;
    private final Long intervalStart;
    private final Long intervalEnd;
    private final Boolean processedInRound;
    private final Integer lastCommittedRow;
    private final long lastUpdatedMs; // For staleness detection
    
    public StripeReportState(Long roundId, String reportRunId, Long intervalStart, 
                             Long intervalEnd, Boolean processedInRound, Integer lastCommittedRow) {
        this.roundId = roundId;
        this.reportRunId = reportRunId;
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.processedInRound = processedInRound;
        this.lastCommittedRow = lastCommittedRow;
        this.lastUpdatedMs = System.currentTimeMillis();
    }
    
    /**
     * Creates a StripeReportState from a StripeSourceOffset
     */
    public static StripeReportState fromOffset(StripeSourceOffset offset) {
        if (offset == null) {
            return null;
        }
        return new StripeReportState(
            offset.getRoundId(),
            offset.getInProgressReportRunId(),
            offset.getIntervalStart(),
            offset.getIntervalEnd(),
            offset.getProcessedInRound(),
            offset.getLastCommittedRow()
        );
    }
    
    /**
     * Checks if this state is for the given round ID
     */
    public boolean isForRound(Long currentRoundId) {
        return roundId != null && roundId.equals(currentRoundId);
    }
    
    /**
     * Checks if there's an in-progress report run
     */
    public boolean hasInProgressReport() {
        return reportRunId != null && !reportRunId.isEmpty();
    }
    
    /**
     * Checks if the report is completed for the given round
     */
    public boolean isCompletedForRound(Long currentRoundId) {
        if (currentRoundId < roundId) {
            return true;
        }
        return isForRound(currentRoundId) && Boolean.TRUE.equals(processedInRound);
    }

    /**
     * Checks if this state is stale (older than the given threshold in milliseconds)
     */
    public boolean isStale(long stalenessThresholdMs) {
        long ageMs = System.currentTimeMillis() - lastUpdatedMs;
        return ageMs >= stalenessThresholdMs;
    }
    
    /**
     * Gets the age of this state in milliseconds
     */
    public long getAgeMs() {
        return System.currentTimeMillis() - lastUpdatedMs;
    }
    
    // Getters
    public Long getRoundId() {
        return roundId;
    }
    
    public String getReportRunId() {
        return reportRunId;
    }
    
    public Long getIntervalStart() {
        return intervalStart;
    }
    
    public Long getIntervalEnd() {
        return intervalEnd;
    }
    
    public Integer getLastCommittedRow() {
        return lastCommittedRow;
    }
    
    public long getLastUpdatedMs() {
        return lastUpdatedMs;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StripeReportState that = (StripeReportState) o;
        return Objects.equals(roundId, that.roundId) &&
               Objects.equals(reportRunId, that.reportRunId) &&
               Objects.equals(intervalStart, that.intervalStart) &&
               Objects.equals(intervalEnd, that.intervalEnd) &&
               Objects.equals(lastCommittedRow, that.lastCommittedRow);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(roundId, reportRunId, intervalStart, intervalEnd, lastCommittedRow);
    }
    
    @Override
    public String toString() {
        return "StripeReportState{" +
                "roundId=" + roundId +
                ", reportRunId='" + reportRunId + '\'' +
                ", intervalStart=" + intervalStart +
                ", intervalEnd=" + intervalEnd +
                ", lastCommittedRow=" + lastCommittedRow +
                ", processedInRound=" + processedInRound +
                ", ageMs=" + getAgeMs() +
                '}';
    }
}

