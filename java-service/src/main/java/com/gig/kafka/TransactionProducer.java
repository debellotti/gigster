package com.gig.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String RAW_TOPIC = "transactions-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(String messageJson) {
        kafkaTemplate.send(RAW_TOPIC, messageJson);
        log.debug("Published to {}: {}", RAW_TOPIC, messageJson);
    }

    public void send(String topic, String messageJson) {
        kafkaTemplate.send(topic, messageJson);
        log.debug("Published to {}: {}", topic, messageJson);
    }
}
