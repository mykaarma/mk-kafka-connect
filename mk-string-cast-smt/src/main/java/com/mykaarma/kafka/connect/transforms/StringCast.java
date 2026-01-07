package com.mykaarma.kafka.connect.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple Kafka Connect SMT to cast specified fields to strings in schemaless records.
 * Handles only the record value (not keys) and silently skips non-existent fields.
 */
public abstract class StringCast<R extends ConnectRecord<R>> implements Transformation<R> {
    
    private static final Logger log = LoggerFactory.getLogger(StringCast.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DOC = "Comma-separated list of field names to convert to strings";
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, 
                    ConfigDef.Type.STRING, 
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, 
                    FIELDS_DOC);
    
    private List<String> fields;
    
    @Override
    public void configure(Map<String, ?> configs) {
        String fieldsConfig = (String) configs.get(FIELDS_CONFIG);
        
        if (fieldsConfig == null || fieldsConfig.trim().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one field to cast");
        }
        
        // Parse comma-separated field names
        fields = Arrays.asList(fieldsConfig.split("\\s*,\\s*"));
        
        log.info("StringCast configured with fields: {}", fields);
    }
    
    @Override
    public R apply(R record) {
        // Get the operating value (key or value depending on subclass)
        Object value = operatingValue(record);
        
        // If value is null or record has schema, return as-is
        if (value == null || operatingSchema(record) != null) {
            return record;
        }
        
        // Handle only schemaless Map records
        if (!(value instanceof Map)) {
            log.warn("Value is not a Map, skipping transformation");
            return record;
        }
        
        @SuppressWarnings("unchecked")
        Map<String, Object> originalMap = (Map<String, Object>) value;
        Map<String, Object> updatedMap = new HashMap<>(originalMap);
        
        // Convert specified fields to strings
        for (String field : fields) {
            if (updatedMap.containsKey(field)) {
                Object fieldValue = updatedMap.get(field);
                if (fieldValue != null) {
                    updatedMap.put(field, convertToString(fieldValue));
                    log.debug("Converted field '{}' to string", field);
                }
            } else {
                log.debug("Field '{}' not found in record, skipping", field);
            }
        }
        
        return newRecord(record, updatedMap);
    }
    
    /**
     * Convert any value to JSON string using Jackson ObjectMapper.
     */
    private String convertToString(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new DataException("Failed to convert value to JSON string: " + e.getMessage(), e);
        }
    }
    
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
    
    @Override
    public void close() {
        // No resources to clean up
    }
    
    // Abstract methods to be implemented by Key/Value subclasses
    protected abstract Object operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Object updatedValue);
    
    /**
     * Value transformer - transforms the record value
     */
    public static final class Value<R extends ConnectRecord<R>> extends StringCast<R> {
        
        @Override
        protected Object operatingSchema(R record) {
            return record.valueSchema();
        }
        
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
        
        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null, // schemaless
                    updatedValue,
                    record.timestamp()
            );
        }
    }
}

