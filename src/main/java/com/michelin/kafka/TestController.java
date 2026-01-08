package com.michelin.kafka;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class TestController {

    private final Logger otelLogger;

    public TestController() {
        this.otelLogger = GlobalOpenTelemetry.get().getLogsBridge().get("com.michelin.kafka.TestController");
    }

    @GetMapping("/test-log")
    public String sendTestLog() {
        String testId = UUID.randomUUID().toString();
        
        otelLogger.logRecordBuilder()
                .setSeverity(Severity.INFO)
                .setBody("TEST OTLP LOG: This is a manual test log with ID: " + testId)
                .setAllAttributes(Attributes.builder().put("test.id", testId).build())
                .emit();

        otelLogger.logRecordBuilder()
                .setSeverity(Severity.ERROR)
                .setBody("TEST OTLP ERROR: This is a manual test error with ID: " + testId)
                .setAllAttributes(Attributes.builder().put("test.id", testId).build())
                .emit();

        return "Sent test logs with ID: " + testId + ". Check Grafana.";
    }
}
