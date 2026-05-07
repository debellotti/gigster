package com.gig.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gig.model.Transaction;
import com.gig.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "gig-consumer-group")
    public void consumeProcessedTransaction(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);

            String transactionId = getField(node, "transaction_id");
            String userId = getField(node, "account_id");
            String amountStr = getField(node, "amount");
            String currency = getField(node, "currency");
            String timestamp = getField(node, "timestamp");
            String status = getField(node, "status");

            if (transactionId == null || transactionId.isEmpty()) {
                log.warn("Skipping record with missing transaction_id");
                return;
            }

            BigDecimal amount;
            try {
                amount = new BigDecimal(amountStr);
            } catch (Exception e) {
                log.warn("Skipping record with invalid amount: {}", amountStr);
                return;
            }

            Optional<Transaction> existing = transactionRepository.findByTransactionId(transactionId);
            if (existing.isPresent()) {
                if (amount.compareTo(existing.get().getAmount()) <= 0) return;
                transactionRepository.delete(existing.get());
            }

            Transaction tx = new Transaction();
            tx.setTransactionId(transactionId);
            tx.setAccountId(userId);
            tx.setAmount(amount);
            tx.setCurrency(currency);
            tx.setStatus(status);
            tx.setTimestamp(parseTimestamp(timestamp));

            transactionRepository.save(tx);
            log.info("Saved transaction: {}", transactionId);

        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
        }
    }

    private String getField(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asText() : null;
    }

    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null) return LocalDateTime.now();
        try {
            return LocalDateTime.parse(timestamp.replace("Z", ""),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }
}
