package com.michelin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConnectLogListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);
    private final Map<TopicPartition, Long> seekOffsets = new ConcurrentHashMap<>();

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "#{@effectiveGroupId}")
    public void listen(ConsumerRecord<String, String> record, Consumer<?, ?> consumer) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());

        // Check if we have already calculated the seek offset for this partition
        if (!seekOffsets.containsKey(topicPartition)) {
            // Get the end offset for the partition
            Long endOffset = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
            
            // Calculate the target offset (end - 3)
            // Ensure we don't seek to a negative offset
            long targetOffset = Math.max(0, endOffset - 3);
            
            seekOffsets.put(topicPartition, targetOffset);

            // Seek only if we are currently behind the target
            // (or to force a replay if we are ahead, but usually we just want to ensure we are AT LEAST at target)
            // However, to strictly "show last 3", we should seek to target.
            // But if we seek, we must return to avoid processing the current record if it's too old.
            consumer.seek(topicPartition, targetOffset);
            
            // If the current record is older than our target, skip it.
            if (record.offset() < targetOffset) {
                return;
            }
        }

        // Skip records that are older than our target offset
        // This is necessary because the consumer might have already fetched a batch of records
        // before we performed the seek.
        if (record.offset() < seekOffsets.get(topicPartition)) {
            return;
        }

        // Log the record value. OTel agent will pick this up from the logs.
        logger.info("Received Connect Log Event: key={}, value={}, partition={}, offset={}", 
            record.key(), record.value(), record.partition(), record.offset());
    }
}
