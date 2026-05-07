package com.gig.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionProducer {

    private static final String RAW_TOPIC = "transactions-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(String messageJson) {
        kafkaTemplate.send(RAW_TOPIC, messageJson);
    }
}
