package com.michelin.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConnectLogListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        // Log the record value. OTel agent will pick this up from the logs.
        logger.info("Received Connect Log Event: key={}, value={}, partition={}, offset={}", 
            record.key(), record.value(), record.partition(), record.offset());
    }
}


