package com.michelin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConnectLogListener implements ConsumerAwareRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(ConnectLogListener.class);
    // Track which partitions we have already initialized (seeked)
    private final Map<TopicPartition, Boolean> initializedPartitions = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            // Only seek if we haven't done so for this partition in this session
            if (initializedPartitions.putIfAbsent(partition, true) == null) {
                Long endOffset = consumer.endOffsets(java.util.Collections.singleton(partition)).get(partition);
                if (endOffset != null) {
                    long targetOffset = Math.max(0, endOffset - 3);
                    logger.info("Seeking partition {} to offset {} (end offset was {})", partition, targetOffset, endOffset);
                    consumer.seek(partition, targetOffset);
                }
            }
        }
    }

    @KafkaListener(topics = "${connect.logs.topic}", groupId = "#{@effectiveGroupId}")
    public void listen(ConsumerRecord<String, String> record) {
        // Log the record value. OTel agent will pick this up from the logs.
        logger.info("Received Connect Log Event: key={}, value={}, partition={}, offset={}", 
            record.key(), record.value(), record.partition(), record.offset());
    }
}
