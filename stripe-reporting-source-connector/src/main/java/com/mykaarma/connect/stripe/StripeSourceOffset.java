package com.mykaarma.connect.stripe;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the source offset for Stripe reporting connector
 * Tracks the progress of report processing including intervals, in-progress state, and round tracking
 */
public class StripeSourceOffset {
    
    private Long intervalStart;
    private Long intervalEnd;
    private String inProgressReportRunId;
    private Integer lastCommittedRow;
    private Long roundId;
    private Boolean processedInRound;
    
    public StripeSourceOffset(Long intervalStart, Long intervalEnd, String inProgressReportRunId, Integer lastCommittedRow) {
        this(intervalStart, intervalEnd, inProgressReportRunId, lastCommittedRow, null, null);
    }
    
    public StripeSourceOffset(Long intervalStart, Long intervalEnd, String inProgressReportRunId, Integer lastCommittedRow,
                              Long roundId, Boolean processedInRound) {
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.inProgressReportRunId = inProgressReportRunId;
        this.lastCommittedRow = lastCommittedRow;
        this.roundId = roundId;
        this.processedInRound = processedInRound;
    }
    
    public Long getIntervalStart() {
        return intervalStart;
    }

    public void setIntervalStart(Long intervalStart) {
        this.intervalStart = intervalStart;
    }
    
    public Long getIntervalEnd() {
        return intervalEnd;
    }
    
    public String getInProgressReportRunId() {
        return inProgressReportRunId;
    }
    
    public void setInProgressReportRunId(String inProgressReportRunId) {
        this.inProgressReportRunId = inProgressReportRunId;
    }

    public Integer getLastCommittedRow() {
        return lastCommittedRow;
    }
    
    public void setLastCommittedRow(Integer lastCommittedRow) {
        this.lastCommittedRow = lastCommittedRow;
    }

    public Long getRoundId() {
        return roundId;
    }

    public void setRoundId(Long roundId) {
        this.roundId = roundId;
    }

    public void setProcessedInRound(Boolean processedInRound) {
        this.processedInRound = processedInRound;
    }

    public Boolean getProcessedInRound() {
        return processedInRound;
    }

    /**
     * Checks if there is an in-progress report
     * 
     * @return true if in_progress_report_run_id is not null
     */
    public boolean hasInProgressReport() {
        return inProgressReportRunId != null && !inProgressReportRunId.isEmpty();
    }
    
    /**
     * Checks if this combination was processed in the given round
     * 
     * @param currentRoundId The current round ID to check against
     * @return true if processedInRound is true and roundId matches currentRoundId
     */
    public boolean isProcessedInRound(Long currentRoundId) {
        if (currentRoundId == null || currentRoundId == 0L) {
            return false;
        }
        if (roundId > currentRoundId) {
            // the checked round is already completed, and we are in the next round
            return true;
        }
        return Boolean.TRUE.equals(processedInRound) && currentRoundId.equals(roundId);
    }
    
    /**
     * Converts this offset to a Map for use in SourceRecord
     * 
     * @return Map representation of the offset
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("interval_start", intervalStart);
        map.put("interval_end", intervalEnd);
        map.put("in_progress_report_run_id", inProgressReportRunId);
        map.put("last_committed_row", lastCommittedRow);
        map.put("round_id", roundId);
        map.put("processed_in_round", processedInRound);
        return map;
    }
    
    /**
     * Creates a StripeSourceOffset from a Map
     * Handles various number types and string conversions
     * 
     * @param map Map containing offset data
     * @return StripeSourceOffset instance, or null if map is null
     */
    public static StripeSourceOffset fromMap(Map<String, Object> map) {
        if (map == null) {
            return null;
        }
        
        Long intervalStart = extractLong(map.get("interval_start"));
        Long intervalEnd = extractLong(map.get("interval_end"));
        String inProgressReportRunId = extractString(map.get("in_progress_report_run_id"));
        Integer lastCommittedRow = extractInteger(map.get("last_committed_row"));
        Long roundId = extractLong(map.get("round_id"));
        Boolean processedInRound = extractBoolean(map.get("processed_in_round"));
        
        return new StripeSourceOffset(intervalStart, intervalEnd, inProgressReportRunId, lastCommittedRow,
                roundId, processedInRound);
    }
    
    /**
     * Extracts a Long value from an Object, handling Number and String types
     */
    private static Long extractLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Extracts an Integer value from an Object, handling Number and String types
     */
    private static Integer extractInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Extracts a Boolean value from an Object, handling Boolean, Number, and String types
     */
    private static Boolean extractBoolean(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof String) {
            String str = ((String) value).trim().toLowerCase();
            return "true".equals(str) || "1".equals(str) || "yes".equals(str);
        }
        return null;
    }
    
    /**
     * Extracts a String value from an Object
     */
    private static String extractString(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }
}

