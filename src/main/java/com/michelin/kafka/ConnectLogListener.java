package com.michelin.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Severity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConnectLogListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final io.opentelemetry.api.logs.Logger otelLogger;

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
            
            JsonNode dataNode = rootNode.path("data");
            String level = dataNode.path("level").asText("INFO");
            String connectorId = dataNode.path("context").path("connectorId").asText("unknown");
            
            // Build attributes
            AttributesBuilder attributes = Attributes.builder()
                .put("event.type", eventType)
                .put("event.id", eventId)
                .put("event.source", eventSource)
                .put("event.time", eventTime)
                .put("connector.id", connectorId)
                .put("kafka.partition", record.partition())
                .put("kafka.offset", record.offset());

            String message = "Connect Log Event";
            
            // Extract error details if present
            if (dataNode.has("summary")) {
                JsonNode summary = dataNode.path("summary");
                if (summary.has("connectorErrorSummary")) {
                    JsonNode errorSummary = summary.path("connectorErrorSummary");
                    String errorMsg = errorSummary.path("message").asText("");
                    String rootCause = errorSummary.path("rootCause").asText("");
                    
                    attributes.put("connector.error.message", errorMsg);
                    attributes.put("connector.error.root_cause", rootCause);
                    message = errorMsg.isEmpty() ? message : errorMsg;
                }
            }

            // Map level to Severity
            Severity severity = mapSeverity(level);
            
            // Build final attributes
            Attributes builtAttributes = attributes.build();

            // Log what we are sending locally
            logger.info("Sending OTel Log - EventID: {}, Severity: {}, Message: {}", eventId, severity, message);
            logger.debug("OTel Attributes: {}", builtAttributes);
            
            // Emit OTel log
            otelLogger.logRecordBuilder()
                .setSeverity(severity)
                .setSeverityText(level)
                .setBody(message)
                .setAllAttributes(builtAttributes)
                .emit();

            logger.info("OTel log emitted successfully for event: {}", eventId);

        } catch (Exception e) {
            logger.error("Failed to parse/process connect log event", e);
        }
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
