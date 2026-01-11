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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ConnectLogListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final io.opentelemetry.api.logs.Logger otelLogger;

    // Regex to extract environment and connector from the Confluent CRN source string
    private static final Pattern ENV_PATTERN = Pattern.compile("environment=([^/]+)");
    private static final Pattern CONN_PATTERN = Pattern.compile("connector=([^/]+)");

    public ConnectLogListener() {
        this.otelLogger = GlobalOpenTelemetry.get().getLogsBridge().get("com.michelin.kafka.ConnectLogListener");
    }

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "#{@effectiveGroupId}")
    public void listen(ConsumerRecord<String, String> record) {
        String value = record.value();
        String key = record.key() == null ? "null" : record.key();

        // 1. RESTORE DETAILED SLF4J LOGGING
        // This makes the console/file output look exactly like your previous logs
        logger.info("Received Connect Log Event: key={}, value={}, partition={}, offset={}",
                key, value, record.partition(), record.offset());

        try {
            JsonNode rootNode = objectMapper.readTree(value);

            String eventType = rootNode.path("type").asText("unknown");
            String eventSource = rootNode.path("source").asText("");
            String eventTime = rootNode.path("time").asText();

            JsonNode dataNode = rootNode.path("data");
            String level = dataNode.path("level").asText("INFO");
            String connectorId = dataNode.path("context").path("connectorId").asText("unknown");

            // Extract Environment and Connector from the source string
            String environment = "unknown";
            String connector = "unknown";
            Matcher envMatcher = ENV_PATTERN.matcher(eventSource);
            if (envMatcher.find()) environment = envMatcher.group(1);
            Matcher connMatcher = CONN_PATTERN.matcher(eventSource);
            if (connMatcher.find()) connector = connMatcher.group(1);

            // 2. BUILD ATTRIBUTES FOR GRAFANA TABLE
            AttributesBuilder attributes = Attributes.builder()
                    .put("event.type", eventType)
                    .put("event.source", eventSource)
                    .put("event.time", eventTime)
                    .put("connector.id", connectorId)
                    .put("connector.name", connector) // For the table
                    .put("environment", environment)   // For the table
                    .put("kafka.partition", (long) record.partition())
                    .put("kafka.offset", record.offset());

            String bodyMessage = "Connect Log Event";

            if (dataNode.has("summary")) {
                JsonNode summary = dataNode.path("summary");
                // Put the whole summary object as a string for the Grafana table
                attributes.put("summary", summary.toString());

                if (summary.has("connectorErrorSummary")) {
                    String errorMsg = summary.path("connectorErrorSummary").path("message").asText("");
                    bodyMessage = errorMsg.isEmpty() ? bodyMessage : errorMsg;
                }
            }

            // Emit OTel log
            otelLogger.logRecordBuilder()
                    .setSeverity(mapSeverity(level))
                    .setSeverityText(level)
                    .setBody(bodyMessage)
                    .setAllAttributes(attributes.build())
                    .emit();

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