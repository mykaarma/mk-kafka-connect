package com.mykaarma.connect.stripe;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the source partition for Stripe reporting connector
 * Used to identify which report type and timezone a record belongs to
 */
public class StripeSourcePartition {
    
    private final String reportType;
    private final String timezone;
    
    public StripeSourcePartition(String reportType, String timezone) {
        this.reportType = reportType;
        this.timezone = timezone;
    }
    
    public String getReportType() {
        return reportType;
    }
    
    public String getTimezone() {
        return timezone;
    }
    
    /**
     * Converts this partition to a Map for use in SourceRecord
     * 
     * @return Map representation of the partition
     */
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("report_type", reportType);
        map.put("timezone", timezone);
        return map;
    }
    
    /**
     * Creates a StripeSourcePartition from a Map
     * 
     * @param map Map containing partition data
     * @return StripeSourcePartition instance
     */
    public static StripeSourcePartition fromMap(Map<String, String> map) {
        if (map == null) {
            return null;
        }
        return new StripeSourcePartition(
                map.get("report_type"),
                map.get("timezone")
        );
    }
}

