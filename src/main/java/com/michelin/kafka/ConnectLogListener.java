package com.michelin.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Severity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ConnectLogListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final io.opentelemetry.api.logs.Logger otelLogger;
    
    // Pattern to extract environment from source string like:
    // crn://confluent.cloud/environment=env-mwvgw/kafka=lkc-kz3jm/connector=lcc-8wypzm
    private static final Pattern ENV_PATTERN = Pattern.compile("environment=([^/]+)");

    public ConnectLogListener() {
        this.otelLogger = GlobalOpenTelemetry.get().getLogsBridge().get("com.michelin.kafka.ConnectLogListener");
    }

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "#{@effectiveGroupId}")
    public void listen(ConsumerRecord<String, String> record) {
        String value = record.value();
        
        // standard local logging (won't be exported to Grafana due to configuration)
        logger.info("Received Connect Log Event: partition={}, offset={}", record.partition(), record.offset());
        logger.debug("Payload: {}", value);

        try {
            JsonNode rootNode = objectMapper.readTree(value);
            
            // Extract core fields
            String eventType = rootNode.path("type").asText("unknown");
            String eventId = rootNode.path("id").asText("unknown");
            String eventSource = rootNode.path("source").asText("unknown");
            String eventTime = rootNode.path("time").asText();
            String environment = extractEnvironment(eventSource);
            
            JsonNode dataNode = rootNode.path("data");
            String level = dataNode.path("level").asText("INFO");
            String connectorId = dataNode.path("context").path("connectorId").asText("unknown");
            
            // Construct the structured body
            ObjectNode logBody = objectMapper.createObjectNode();
            logBody.put("event_type", eventType);
            logBody.put("event_id", eventId);
            logBody.put("event_source", eventSource);
            logBody.put("event_time", eventTime);
            logBody.put("environment", environment);
            logBody.put("connector_id", connectorId);
            logBody.put("kafka_partition", record.partition());
            logBody.put("kafka_offset", record.offset());

            String message = "Connect Log Event";
            
            // Extract error details if present
            if (dataNode.has("summary")) {
                JsonNode summary = dataNode.path("summary");
                if (summary.has("connectorErrorSummary")) {
                    JsonNode errorSummary = summary.path("connectorErrorSummary");
                    String errorMsg = errorSummary.path("message").asText("");
                    String rootCause = errorSummary.path("rootCause").asText("");
                    
                    logBody.put("connector_error_message", errorMsg);
                    logBody.put("connector_error_root_cause", rootCause);
                    message = errorMsg.isEmpty() ? message : errorMsg;
                }
            }
            
            // Add the human-readable message as well
            logBody.put("message", message);

            // Map level to Severity
            Severity severity = mapSeverity(level);
            
            // Emit OTel log with JSON Body
            otelLogger.logRecordBuilder()
                .setSeverity(severity)
                .setSeverityText(level)
                .setBody(objectMapper.writeValueAsString(logBody))
                .emit();

            logger.info("OTel log emitted successfully for event: {}", eventId);

        } catch (Exception e) {
            logger.error("Failed to parse/process connect log event", e);
        }
    }

    private String extractEnvironment(String source) {
        if (source == null) return "unknown";
        Matcher matcher = ENV_PATTERN.matcher(source);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "unknown";
    }

    private Severity mapSeverity(String level) {
        if (level == null) return Severity.INFO;
        return switch (level.toUpperCase()) {
            case "ERROR", "FATAL" -> Severity.ERROR;
            case "WARN", "WARNING" -> Severity.WARN;
            case "DEBUG" -> Severity.DEBUG;
            case "TRACE" -> Severity.TRACE;
            default -> Severity.INFO;
        };
    }
}
