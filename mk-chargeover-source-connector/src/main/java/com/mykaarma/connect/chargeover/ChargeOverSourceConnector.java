package com.mykaarma.connect.chargeover;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ChargeOverSourceConnector is a Kafka Connect source connector for streaming
 * change data from ChargeOver API to Kafka topics.
 */
public class ChargeOverSourceConnector extends SourceConnector {
    
    private static final Logger log = LoggerFactory.getLogger(ChargeOverSourceConnector.class);
    
    public static final String CHARGEOVER_SUBDOMAIN_CONFIG = "chargeover.subdomain";
    public static final String CHARGEOVER_USERNAME_CONFIG = "chargeover.username";
    public static final String CHARGEOVER_PASSWORD_CONFIG = "chargeover.password";
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String ENTITIES_CONFIG = "chargeover.entities";
    public static final String ENTITY_ID_FIELDS_CONFIG = "chargeover.entity.id.fields";
    
    private Map<String, String> configProps;
    
    @Override
    public String version() {
        return "1.0.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting ChargeOver Source Connector");
        this.configProps = props;
        
        // Validate configuration
        ChargeOverSourceConnectorConfig config = new ChargeOverSourceConnectorConfig(props);
        log.info("Connector configured with subdomain: {}", config.getSubdomain());
        log.info("Tracking entities: {}", config.getEntities());
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return ChargeOverSourceTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        
        // For simplicity, we'll create identical configs for each task
        // In a more advanced implementation, you could partition entities across tasks
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            taskConfigs.add(taskConfig);
        }
        
        return taskConfigs;
    }
    
    @Override
    public void stop() {
        log.info("Stopping ChargeOver Source Connector");
    }
    
    @Override
    public ConfigDef config() {
        return ChargeOverSourceConnectorConfig.configDef();
    }
}

