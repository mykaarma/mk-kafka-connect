package com.mykaarma.connect.chargeover;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * HTTP client for interacting with ChargeOver API
 */
public class ChargeOverApiClient {
    
    private static final Logger log = LoggerFactory.getLogger(ChargeOverApiClient.class);
    
    private final String apiUrl;
    private final String username;
    private final String password;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    /**
     * Result wrapper for API fetch operations
     */
    public static class FetchResult {
        private final List<JsonNode> records;
        private final boolean hasMore;
        private final int totalFetched;
        
        public FetchResult(List<JsonNode> records, boolean hasMore, int totalFetched) {
            this.records = records;
            this.hasMore = hasMore;
            this.totalFetched = totalFetched;
        }
        
        public List<JsonNode> getRecords() {
            return records;
        }
        
        public boolean hasMore() {
            return hasMore;
        }
        
        public int getTotalFetched() {
            return totalFetched;
        }
    }
    
    public ChargeOverApiClient(String apiUrl, String username, String password) {
        this.apiUrl = apiUrl.endsWith("/") ? apiUrl : apiUrl + "/";
        this.username = username;
        this.password = password;
        this.httpClient = HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Fetch changes for a specific entity type with pagination support
     * 
     * @param entity Entity type (e.g., customer, invoice, payment)
     * @param datetimeField Field name to use for datetime filtering (e.g., mod_datetime, updated_at)
     * @param startDatetime Start datetime in YYYY-MM-DD HH:MM:SS format (inclusive), or null for all records
     * @param endDatetime End datetime in YYYY-MM-DD HH:MM:SS format (exclusive), or null for no upper bound
     * @param offset Pagination offset (0 for first page)
     * @param limit Maximum number of records to fetch
     * @param additionalQueryParams Additional query parameters to append to URL (e.g., "expand=items&fields=id,name")
     * @return FetchResult containing records and pagination metadata
     */
    public FetchResult fetchChangesWithPagination(String entity, String datetimeField, String startDatetime, 
                                                   String endDatetime, int offset, Integer limit, String additionalQueryParams) throws IOException {
        List<JsonNode> results = new ArrayList<>();
        
        // Build the API endpoint URL
        // ChargeOver API format: https://your-company.chargeover.com/api/v3/{entity}?where=...&order=...&limit=...&offset=...
        StringBuilder urlBuilder = new StringBuilder(apiUrl);
        urlBuilder.append(entity);
        urlBuilder.append("?limit=").append(limit);
        urlBuilder.append("&offset=").append(offset);
        
        // Build where clause for datetime filter
        // ChargeOver API requires colons in datetime to be escaped with backslash
        // Format: <datetimeField>:GTE:2025-01-01 01\:00\:00
        List<String> whereConditions = new ArrayList<>();
        
        if (startDatetime != null && !startDatetime.isEmpty()) {
            // Escape colons in datetime: "2025-01-01 01:00:00" -> "2025-01-01 01\:00\:00"
            String escapedStartDatetime = startDatetime.replace(":", "\\:");
            String encodedStartDatetime = URLEncoder.encode(escapedStartDatetime, StandardCharsets.UTF_8);
            whereConditions.add(datetimeField + ":GTE:" + encodedStartDatetime);
        }
        
        if (endDatetime != null && !endDatetime.isEmpty()) {
            // Escape colons in datetime: "2025-01-01 01:00:00" -> "2025-01-01 01\:00\:00"
            String escapedEndDatetime = endDatetime.replace(":", "\\:");
            String encodedEndDatetime = URLEncoder.encode(escapedEndDatetime, StandardCharsets.UTF_8);
            whereConditions.add(datetimeField + ":LT:" + encodedEndDatetime);
        }
        
        // Combine where conditions with comma separator
        if (!whereConditions.isEmpty()) {
            urlBuilder.append("&where=").append(String.join(",", whereConditions));
        }
        
        // Add ordering by datetime field
        urlBuilder.append("&order=").append(datetimeField).append(":ASC");
        
        // Append additional query parameters if provided
        if (additionalQueryParams != null && !additionalQueryParams.isEmpty()) {
            urlBuilder.append("&").append(additionalQueryParams);
            log.debug("Appending additional query params for entity {}: {}", entity, additionalQueryParams);
        }
        
        String url = urlBuilder.toString();
        log.info("ChargeOver API Call: {} [entity={}, startDatetime={}, endDatetime={}, offset={}, limit={}]", 
                url, entity, 
                startDatetime != null ? startDatetime : "none",
                endDatetime != null ? endDatetime : "none",
                offset, limit);
        
        HttpGet request = new HttpGet(url);
        
        // Set authentication headers
        // ChargeOver uses Basic Auth with username:password
        String auth = username + ":" + password;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + encodedAuth);
        request.setHeader("Content-Type", "application/json");
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            String responseBody = EntityUtils.toString(response.getEntity());
            
            if (statusCode == 200) {
                JsonNode rootNode = objectMapper.readTree(responseBody);
                
                // ChargeOver API response format varies, adjust based on actual API response
                // Typically: {"response": [...records...]}
                if (rootNode.has("response") && rootNode.get("response").isArray()) {
                    for (JsonNode record : rootNode.get("response")) {
                        results.add(record);
                    }
                } else if (rootNode.isArray()) {
                    // Some APIs return array directly
                    for (JsonNode record : rootNode) {
                        results.add(record);
                    }
                }
                
                log.debug("Successfully fetched {} records for entity: {} at offset: {} (startDatetime: {}, endDatetime: {})", 
                         results.size(), entity, offset, startDatetime, endDatetime);
                
                // Determine if there are more records
                // If we got exactly 'limit' records, there might be more
                boolean hasMore = results.size() == limit;
                
                return new FetchResult(results, hasMore, results.size());
                
            } else if (statusCode == 429) {
                log.warn("Rate limit exceeded for entity: {} at offset: {}", entity, offset);
                throw new ChargeOverRateLimitException("Rate limit exceeded: " + responseBody);
            } else {
                log.error("API request failed with status {}: {}", statusCode, responseBody);
                throw new IOException("API request failed with status " + statusCode + ": " + responseBody);
            }
        } catch (IOException e) {
            log.error("Error fetching changes for entity: {} at offset: {}", entity, offset, e);
            throw e;
        } catch (Exception e) {
            log.error("Error fetching changes for entity: {} at offset: {}", entity, offset, e);
            throw new IOException("Failed to fetch changes from ChargeOver API", e);
        }
    }
    
    /**
     * Test the API connection
     * 
     * @return true if connection is successful
     */
    public boolean testConnection() {
        try {
            // Try to fetch a simple endpoint to test connectivity
            HttpGet request = new HttpGet(apiUrl + "customer?limit=1");
            
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            request.setHeader("Authorization", "Basic " + encodedAuth);
            request.setHeader("Content-Type", "application/json");
            
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getCode();
                log.info("Connection test returned status: {}", statusCode);
                return statusCode == 200;
            }
        } catch (Exception e) {
            log.error("Connection test failed", e);
            return false;
        }
    }
    
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (IOException e) {
            log.error("Error closing HTTP client", e);
        }
    }
}

