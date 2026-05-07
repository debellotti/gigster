package com.gig.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gig.model.TransactionTarget;
import com.gig.repository.TransactionTargetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simulates the Apache NiFi Phase 2 transformation layer.
 *
 * Consumes raw transactions from transactions-topic (CSV field format),
 * persists to transactions_target (simulating NiFi DB write),
 * and republishes to transactions-processed with renamed fields
 * (user_id → account_id, transaction_date → timestamp) for the Java consumer.
 */
@Component
public class NiFiTransformerService {

    private static final Logger log = LoggerFactory.getLogger(NiFiTransformerService.class);
    private static final String PROCESSED_TOPIC = "transactions-processed";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TransactionProducer producer;

    @Autowired
    private TransactionTargetRepository targetRepository;

    @KafkaListener(topics = "transactions-topic", groupId = "nifi-transformer-group")
    public void transform(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);

            String transactionId = text(node, "transaction_id");
            String userId = text(node, "user_id");
            String amountStr = text(node, "amount");
            String currency = text(node, "currency");
            String transactionDate = text(node, "transaction_date");
            String status = text(node, "status");
            String description = text(node, "description");

            if (transactionId == null || transactionId.isEmpty()) {
                log.warn("Skipping message with missing transaction_id");
                return;
            }

            // Write to transactions_target (idempotent)
            if (targetRepository.findByTransactionId(transactionId).isEmpty()) {
                TransactionTarget target = new TransactionTarget();
                target.setTransactionId(transactionId);
                target.setUserId(userId);
                target.setAmount(new BigDecimal(amountStr));
                target.setCurrency(currency);
                target.setTransactionDate(parseDate(transactionDate));
                target.setStatus(status);
                target.setDescription(description);
                targetRepository.save(target);
                log.info("NiFi: saved to transactions_target: {}", transactionId);
            }

            log.info("NiFi transformer: persisted {} to transactions_target", transactionId);

        } catch (Exception e) {
            log.error("NiFi transformer error: {}", e.getMessage(), e);
        }
    }

    private String text(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asText() : null;
    }

    private LocalDateTime parseDate(String date) {
        if (date == null) return LocalDateTime.now();
        try {
            return LocalDateTime.parse(date.replace("Z", ""),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }
}
