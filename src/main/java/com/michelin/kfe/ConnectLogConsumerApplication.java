package com.michelin.kfe;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

@SpringBootApplication
public class ConnectLogConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConnectLogConsumerApplication.class, args);
    }

    @Bean
    public String effectiveGroupId(@Value("${connect.logs.consume-from-start}") boolean consumeFromStart,
                                   @Value("${spring.kafka.consumer.group-id}") String groupId) {
        if (consumeFromStart) {
            return groupId + "-" + UUID.randomUUID();
        }
        return groupId;
    }

}
