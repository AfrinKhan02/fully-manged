package com.michelin.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class ConnectLogListener {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ConnectLogListener.class);
    private final ObjectMapper objectMapper;
    private final Logger otelLogger;

    public ConnectLogListener(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        // Obtain an OTel logger instance
        this.otelLogger = GlobalOpenTelemetry.get().getLogsBridge().get("com.michelin.kafka.ConnectLogListener");
    }

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "#{@effectiveGroupId}")
    public void listen(ConsumerRecord<String, String> record) {
        try {
            JsonNode root = objectMapper.readTree(record.value());
            
            // Extract core fields
            String dataLevel = root.path("data").path("level").asText("INFO");
            String timeStr = root.path("time").asText();
            String connectorId = root.path("data").path("context").path("connectorId").asText("unknown");
            String eventType = root.path("type").asText("unknown");
            
            // Extract message - try to find the most relevant error message
            String message = "Connect Log Event";
            JsonNode data = root.path("data");
            if (data.has("summary") && data.path("summary").has("connectorErrorSummary")) {
                message = data.path("summary").path("connectorErrorSummary").path("message").asText();
            } else if (data.has("message")) {
                message = data.path("message").asText();
            }

            // Build Attributes
            AttributesBuilder attributes = Attributes.builder()
                    .put("connector.id", connectorId)
                    .put("event.type", eventType)
                    .put("kafka.partition", record.partition())
                    .put("kafka.offset", record.offset());

            // Add other context fields if available
            // You can iterate over data.context or add specific known fields
            
            // Map Severity
            Severity severity = mapSeverity(dataLevel);

            // Emit structured log
            otelLogger.logRecordBuilder()
                    .setTimestamp(parseTime(timeStr))
                    .setSeverity(severity)
                    .setSeverityText(dataLevel)
                    .setBody(message)
                    .setAllAttributes(attributes.build())
                    .emit();

            // Optional: keep console logging for local debugging (won't be exported to Grafana due to filter)
            log.info("Processed Connect Log: connectorId={}, level={}", connectorId, dataLevel);

        } catch (Exception e) {
            log.error("Failed to parse connect log event", e);
            // Fallback: emit raw record if parsing fails
            otelLogger.logRecordBuilder()
                    .setSeverity(Severity.ERROR)
                    .setBody("Failed to parse connect log: " + record.value())
                    .emit();
        }
    }

    private Severity mapSeverity(String level) {
        if (level == null) return Severity.INFO;
        switch (level.toUpperCase()) {
            case "ERROR": return Severity.ERROR;
            case "WARN": return Severity.WARN;
            case "DEBUG": return Severity.DEBUG;
            case "TRACE": return Severity.TRACE;
            default: return Severity.INFO;
        }
    }

    private Instant parseTime(String timeStr) {
        try {
            return timeStr != null && !timeStr.isEmpty() ? Instant.parse(timeStr) : Instant.now();
        } catch (Exception e) {
            return Instant.now();
        }
    }
}
