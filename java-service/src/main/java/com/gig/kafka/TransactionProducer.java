package com.gig.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka Producer for Transaction Events
 * Publishes transactions to Kafka topic for Phase 2 consumption
 */
@Component
public class TransactionProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "transactions-topic";

    public TransactionProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Placeholder: Send transaction to Kafka
     */
    public void sendTransaction(Object transaction) {
        // TODO: Implement sending transaction to Kafka
        // kafkaTemplate.send(TOPIC, transaction);
    }
}
