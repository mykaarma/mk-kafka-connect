package com.mykaarma.connect.chargeover;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.quartz.CronExpression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Configuration class for ChargeOver Source Connector
 */
public class ChargeOverSourceConnectorConfig extends AbstractConfig {
    
    public static final String INCREMENTAL_SCHEDULE_CRON_CONFIG = "incremental.schedule.cron";
    public static final String TIMEZONE_CONFIG = "chargeover.timezone";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final String INITIAL_DATETIME_CONFIG = "initial.datetime";
    public static final String INCREMENTAL_DATETIME_FIELDS_CONFIG = "chargeover.incremental.datetime.fields";
    
    public ChargeOverSourceConnectorConfig(Map<?, ?> originals) {
        super(configDef(), originals);
    }
    
    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(ChargeOverSourceConnector.CHARGEOVER_SUBDOMAIN_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "ChargeOver subdomain")
            .define(ChargeOverSourceConnector.CHARGEOVER_USERNAME_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "ChargeOver Username")
            .define(ChargeOverSourceConnector.CHARGEOVER_PASSWORD_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    ConfigDef.Importance.HIGH,
                    "ChargeOver Password")
            .define(ChargeOverSourceConnector.TOPIC_PREFIX_CONFIG,
                    ConfigDef.Type.STRING,
                    "chargeover",
                    ConfigDef.Importance.MEDIUM,
                    "Topic prefix")
            .define(ChargeOverSourceConnector.POLL_INTERVAL_MS_CONFIG,
                    ConfigDef.Type.LONG,
                    60000L,
                    ConfigDef.Importance.MEDIUM,
                    "Poll interval in milliseconds")
            .define(ChargeOverSourceConnector.BATCH_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    500,
                    ConfigDef.Range.between(1, 500),
                    ConfigDef.Importance.MEDIUM,
                    "Batch size for API requests (limit parameter)")
            .define(INCREMENTAL_SCHEDULE_CRON_CONFIG,
                    ConfigDef.Type.STRING,
                    "0 0 0 * * ?", // 12 AM daily default
                    ConfigDef.Importance.MEDIUM,
                    "Quartz cron expression for incremental load schedule (e.g., '0 0 2 * * ?' for 2 AM daily)")
            .define(TIMEZONE_CONFIG,
                    ConfigDef.Type.STRING,
                    "UTC",
                    ConfigDef.Importance.MEDIUM,
                    "Timezone for cron schedule (e.g., 'UTC', 'America/New_York', 'Europe/London')")
            .define(MAX_RETRIES_CONFIG,
                    ConfigDef.Type.INT,
                    3,
                    ConfigDef.Importance.MEDIUM,
                    "Maximum number of retries for failed API requests")
            .define(INITIAL_DATETIME_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Comma-separated entity to initial datetime mappings in YYYY-MM-DD HH:MM:SS format (e.g., customer:2024-01-01 00:00:00,invoice:2024-01-01 00:00:00)")
            .define(ChargeOverSourceConnector.ENTITIES_CONFIG,
                    ConfigDef.Type.LIST,
                    "customer,invoice,payment,subscription",
                    ConfigDef.Importance.HIGH,
                    "Entities to track")
            .define(ChargeOverSourceConnector.ENTITY_ID_FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    "customer:customer_id,invoice:invoice_id,payment:payment_id,subscription:subscription_id",
                    ConfigDef.Importance.MEDIUM,
                    "Entity ID field mappings")
            .define(INCREMENTAL_DATETIME_FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    "customer:mod_datetime,invoice:mod_datetime,payment:mod_datetime,subscription:mod_datetime",
                    ConfigDef.Importance.MEDIUM,
                    "Entity to datetime field mappings (e.g., customer:mod_datetime,invoice:updated_at)");
    }
    
    public String getSubdomain() {
        return getString(ChargeOverSourceConnector.CHARGEOVER_SUBDOMAIN_CONFIG);
    }
    
    public String getApiUrl() {
        String subdomain = getSubdomain();
        return "https://" + subdomain + ".chargeover.com/api/v3/";
    }
    
    public String getUsername() {
        return getString(ChargeOverSourceConnector.CHARGEOVER_USERNAME_CONFIG);
    }
    
    public String getPassword() {
        return getPassword(ChargeOverSourceConnector.CHARGEOVER_PASSWORD_CONFIG).value();
    }
    
    public String getTopicPrefix() {
        return getString(ChargeOverSourceConnector.TOPIC_PREFIX_CONFIG);
    }
    
    public Long getPollIntervalMs() {
        return getLong(ChargeOverSourceConnector.POLL_INTERVAL_MS_CONFIG);
    }
    
    public Integer getBatchSize() {
        return getInt(ChargeOverSourceConnector.BATCH_SIZE_CONFIG);
    }
    
    public List<String> getEntities() {
        return getList(ChargeOverSourceConnector.ENTITIES_CONFIG);
    }
    
    public Map<String, String> getEntityIdFields() {
        String mappingString = getString(ChargeOverSourceConnector.ENTITY_ID_FIELDS_CONFIG);
        Map<String, String> entityIdFields = new HashMap<>();
        
        if (mappingString != null && !mappingString.trim().isEmpty()) {
            String[] mappings = mappingString.split(",");
            for (String mapping : mappings) {
                String[] parts = mapping.trim().split(":");
                if (parts.length == 2) {
                    entityIdFields.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        
        return entityIdFields;
    }
    
    public String getIdFieldForEntity(String entity) {
        Map<String, String> entityIdFields = getEntityIdFields();
        return entityIdFields.getOrDefault(entity, "id"); // fallback to "id" if not specified
    }
    
    public String getIncrementalScheduleCron() {
        return getString(INCREMENTAL_SCHEDULE_CRON_CONFIG);
    }
    
    public String getTimezone() {
        return getString(TIMEZONE_CONFIG);
    }
    
    public CronExpression getIncrementalScheduleCronExpression() {
        String cronString = getIncrementalScheduleCron();
        String timezone = getTimezone();
        try {
            CronExpression cronExpression = new CronExpression(cronString);
            cronExpression.setTimeZone(TimeZone.getTimeZone(timezone));
            return cronExpression;
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid cron expression: " + cronString, e);
        }
    }
    
    public Integer getMaxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }
    
    /**
     * Parse initial datetime mappings from configuration
     * Format: entity:datetime,entity:datetime
     * Example: customer:2024-01-01 00:00:00,invoice:2024-01-01 00:00:00
     * 
     * @return Map of entity name to datetime string (YYYY-MM-DD HH:MM:SS)
     */
    private Map<String, String> parseInitialDatetimes() {
        String mappingString = getString(INITIAL_DATETIME_CONFIG);
        Map<String, String> initialDatetimes = new HashMap<>();
        
        if (mappingString == null || mappingString.trim().isEmpty()) {
            return initialDatetimes;
        }
        
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        datetimeFormat.setTimeZone(TimeZone.getTimeZone(getTimezone()));
        datetimeFormat.setLenient(false); // Strict parsing
        
        String[] mappings = mappingString.split(",");
        for (String mapping : mappings) {
            String[] parts = mapping.trim().split(":", 2); // Split on first colon only
            if (parts.length == 2) {
                String entity = parts[0].trim();
                String datetimeStr = parts[1].trim();
                
                try {
                    // Validate datetime format by parsing
                    datetimeFormat.parse(datetimeStr);
                    initialDatetimes.put(entity, datetimeStr);
                } catch (ParseException e) {
                    throw new IllegalArgumentException(
                        "Invalid datetime format for entity '" + entity + "': " + datetimeStr + 
                        ". Expected YYYY-MM-DD HH:MM:SS format (e.g., 2024-01-01 00:00:00)", e);
                }
            }
        }
        
        return initialDatetimes;
    }
    
    /**
     * Get initial datetime for a specific entity in YYYY-MM-DD HH:MM:SS format.
     * 
     * @param entity Entity name
     * @return Datetime string in YYYY-MM-DD HH:MM:SS format, or null if not configured
     */
    public String getInitialDatetimeForEntity(String entity) {
        Map<String, String> initialDatetimes = parseInitialDatetimes();
        return initialDatetimes.get(entity);
    }
    
    /**
     * Get all configured initial datetimes for entities
     * 
     * @return Map of entity name to datetime string (YYYY-MM-DD HH:MM:SS)
     */
    public Map<String, String> getAllInitialDatetimes() {
        return parseInitialDatetimes();
    }
    
    /**
     * Get entity to datetime field mappings
     * 
     * @return Map of entity name to datetime field name
     */
    public Map<String, String> getIncrementalDatetimeFields() {
        String mappingString = getString(INCREMENTAL_DATETIME_FIELDS_CONFIG);
        Map<String, String> datetimeFields = new HashMap<>();
        
        if (mappingString != null && !mappingString.trim().isEmpty()) {
            String[] mappings = mappingString.split(",");
            for (String mapping : mappings) {
                String[] parts = mapping.trim().split(":");
                if (parts.length == 2) {
                    datetimeFields.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        
        return datetimeFields;
    }
    
    /**
     * Get datetime field for a specific entity
     * 
     * @param entity Entity name
     * @return Datetime field name (defaults to "mod_datetime" if not specified)
     */
    public String getDatetimeFieldForEntity(String entity) {
        Map<String, String> datetimeFields = getIncrementalDatetimeFields();
        return datetimeFields.getOrDefault(entity, "mod_datetime");
    }
    
    /**
     * Get additional query parameters for a specific entity
     * These params will be appended to the API URL as-is.
     * 
     * Config format: chargeover.{entity}.query.params=expand=items&fields=id,name
     * Example: chargeover.customer.query.params=expand=customer_external&fields=id,name,email
     * 
     * @param entity Entity name
     * @return Query parameter string (e.g., "expand=items&fields=id,name"), or empty string if not configured
     */
    public String getQueryParamsForEntity(String entity) {
        String configKey = "chargeover." + entity + ".query.params";
        Object value = originals().get(configKey);
        
        if (value != null) {
            String params = value.toString().trim();
            return params.isEmpty() ? "" : params;
        }
        
        return "";
    }
}

