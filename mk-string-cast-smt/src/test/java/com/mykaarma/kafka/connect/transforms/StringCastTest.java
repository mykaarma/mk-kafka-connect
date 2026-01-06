package com.mykaarma.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StringCastTest {
    
    private StringCast.Value<SourceRecord> transform;
    
    @BeforeEach
    void setUp() {
        transform = new StringCast.Value<>();
    }
    
    @AfterEach
    void tearDown() {
        if (transform != null) {
            transform.close();
        }
    }
    
    @Test
    void testSimpleFieldConversion() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "age,count");
        transform.configure(config);
        
        // Create test record
        Map<String, Object> value = new HashMap<>();
        value.put("name", "John");
        value.put("age", 30);
        value.put("count", 100L);
        value.put("active", true);
        
        SourceRecord record = createRecord(value);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        
        assertEquals("John", result.get("name")); // unchanged
        assertEquals("30", result.get("age")); // converted to string
        assertEquals("100", result.get("count")); // converted to string
        assertEquals(true, result.get("active")); // unchanged
    }
    
    @Test
    void testNonExistentFieldSkipped() {
        // Configure transform with non-existent field
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "nonExistentField,age");
        transform.configure(config);
        
        // Create test record
        Map<String, Object> value = new HashMap<>();
        value.put("age", 25);
        
        SourceRecord record = createRecord(value);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify - should not throw exception
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        
        assertEquals("25", result.get("age"));
        assertFalse(result.containsKey("nonExistentField"));
    }
    
    @Test
    void testNullValueHandling() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "field1");
        transform.configure(config);
        
        // Create record with null value
        SourceRecord record = createRecord(null);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify - should return original record
        assertNull(transformed.value());
    }
    
    @Test
    void testComplexTypesConversion() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "listField,mapField");
        transform.configure(config);
        
        // Create test record with complex types
        Map<String, Object> value = new HashMap<>();
        value.put("listField", Arrays.asList(1, 2, 3));
        
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("key1", "value1");
        value.put("mapField", nestedMap);
        
        SourceRecord record = createRecord(value);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify - complex types converted to JSON strings
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        
        assertTrue(result.get("listField") instanceof String);
        assertTrue(result.get("mapField") instanceof String);
        assertEquals("[1,2,3]", result.get("listField"));
        assertEquals("{\"key1\":\"value1\"}", result.get("mapField"));
    }
    
    @Test
    void testNullFieldValue() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "nullField,age");
        transform.configure(config);
        
        // Create test record with null field
        Map<String, Object> value = new HashMap<>();
        value.put("nullField", null);
        value.put("age", 30);
        
        SourceRecord record = createRecord(value);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify - null field remains null (not converted)
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        
        assertNull(result.get("nullField"));
        assertEquals("30", result.get("age"));
    }
    
    @Test
    void testAlreadyStringField() {
        // Configure transform
        Map<String, Object> config = new HashMap<>();
        config.put("fields", "name");
        transform.configure(config);
        
        // Create test record with string field
        Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        
        SourceRecord record = createRecord(value);
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify - string gets JSON-quoted
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) transformed.value();
        
        assertEquals("\"John Doe\"", result.get("name"));
    }
    
    @Test
    void testConfigurationWithoutFields() {
        // Configure transform without fields
        Map<String, Object> config = new HashMap<>();
        
        // Should throw exception
        assertThrows(IllegalArgumentException.class, () -> {
            transform.configure(config);
        });
    }
    
    private SourceRecord createRecord(Object value) {
        return new SourceRecord(
                null, // sourcePartition
                null, // sourceOffset
                "test-topic",
                0, // partition
                null, // keySchema
                null, // key
                null, // valueSchema (schemaless)
                value,
                System.currentTimeMillis()
        );
    }
}

