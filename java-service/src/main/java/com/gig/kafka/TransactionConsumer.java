package com.gig.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer for Processed Transaction Events
 * Receives processed transactions from Phase 2 (NiFi)
 */
@Component
public class TransactionConsumer {

    /**
     * Placeholder: Listen for processed transactions
     */
    @KafkaListener(topics = "transactions-processed", groupId = "gig-consumer-group")
    public void consumeProcessedTransaction(Object message) {
        // TODO: Implement receiving processed transactions from NiFi
        // - Deserialize message
        // - Store in target table
        // - Update status
    }
}
