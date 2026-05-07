package com.gig.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.kafka.partitions}")
    private int partitions;

    @Value("${app.kafka.replicas}")
    private short replicas;

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(topic).partitions(partitions).replicas(replicas).build();
    }
}
