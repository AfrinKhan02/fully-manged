package com.michelin.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);

    @GetMapping("/test-log")
    public String sendTestLog() {
        String testId = UUID.randomUUID().toString();
        logger.info("TEST OTLP LOG: This is a manual test log with ID: {}", testId);
        logger.error("TEST OTLP ERROR: This is a manual test error with ID: {}", testId);
        return "Sent test logs with ID: " + testId + ". Check Grafana/Console.";
    }
}

