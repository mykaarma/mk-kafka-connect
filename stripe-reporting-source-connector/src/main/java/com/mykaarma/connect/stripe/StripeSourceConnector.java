package com.mykaarma.connect.stripe;

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
 * StripeSourceConnector is a Kafka Connect source connector for streaming
 * reporting data from Stripe API to Kafka topics.
 */
public class StripeSourceConnector extends SourceConnector {
    
    private static final Logger log = LoggerFactory.getLogger(StripeSourceConnector.class);
    
    public static final String STRIPE_TIMEZONE_APIKEY_MAPPING_CONFIG = "stripe.api.keys";
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    public static final String POLL_CRON_CONFIG = "poll.cron";
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String STRIPE_REPORT_TYPES_CONFIG = "stripe.report.types";
    public static final String STRIPE_REPORT_COLUMNS_CONFIG = "stripe.report.columns";
    public static final String STRIPE_REPORT_INITIAL_LOAD_DATES_CONFIG = "stripe.report.initial.load.dates";
    public static final String STRIPE_REPORT_POLL_INTERVAL_MS_CONFIG = "stripe.report.poll.interval.ms";
    public static final String STRIPE_REPORT_MAX_WAIT_MS_CONFIG = "stripe.report.max.wait.ms";
    public static final String STRIPE_REPORT_KEYS_CONFIG = "stripe.report.keys";
    public static final String IN_PROGRESS_POLL_INTERVAL_MS_CONFIG = "in.progress.poll.interval.ms";
    public static final String MAX_REPORT_INTERVAL_DAYS_CONFIG = "max.report.interval.days";
    
    private Map<String, String> configProps;
    
    @Override
    public String version() {
        return "1.0.0";
    }
    
    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Stripe Reporting Source Connector");
        this.configProps = props;

        // Validate configuration
        try {
            new StripeSourceConnectorConfig(props);
        } catch (Exception e) {
            log.error("Error validating configuration: {}", e.getMessage());
        }
        log.info("Connector configured with stripe");
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return StripeSourceTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configurations", maxTasks);
        
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        
        // For simplicity, we'll create identical configs for each task
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            taskConfigs.add(taskConfig);
        }
        
        return taskConfigs;
    }
    
    @Override
    public void stop() {
        log.info("Stopping Stripe Reporting Source Connector");
    }
    
    @Override
    public ConfigDef config() {
        return StripeSourceConnectorConfig.configDef();
    }
}

