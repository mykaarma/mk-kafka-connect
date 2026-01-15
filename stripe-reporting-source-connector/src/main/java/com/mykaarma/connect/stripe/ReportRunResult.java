package com.mykaarma.connect.stripe;

/**
 * Result object containing report run information and file URL
 */
public class ReportRunResult {
    private final String reportRunId;
    private final String fileUrl;
    private final String status;
    
    public ReportRunResult(String reportRunId, String fileUrl, String status) {
        this.reportRunId = reportRunId;
        this.fileUrl = fileUrl;
        this.status = status;
    }
    
    public String getReportRunId() {
        return reportRunId;
    }
    
    public String getFileUrl() {
        return fileUrl;
    }
    
    public String getStatus() {
        return status;
    }
    
    @Override
    public String toString() {
        return "ReportRunResult{" +
                "reportRunId='" + reportRunId + '\'' +
                ", fileUrl='" + fileUrl + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}

