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
import java.util.Optional;

@Component
public class NiFiTransformerService {

    private static final Logger log = LoggerFactory.getLogger(NiFiTransformerService.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TransactionTargetRepository targetRepository;

    @KafkaListener(topics = "transactions-topic", groupId = "nifi-transformer-group")
    public void transform(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);

            String transactionId = text(node, "transaction_id");
            String accountId = text(node, "account_id");
            String amountStr = text(node, "amount");
            String currency = text(node, "currency");
            String timestamp = text(node, "timestamp");
            String status = text(node, "status");
            String description = text(node, "description");

            if (transactionId == null || transactionId.isEmpty()) {
                log.warn("Skipping message with missing transaction_id");
                return;
            }

            BigDecimal amount = new BigDecimal(amountStr);

            Optional<TransactionTarget> existing = targetRepository.findByTransactionId(transactionId);
            if (existing.isPresent()) {
                if (amount.compareTo(existing.get().getAmount()) <= 0) {
                    log.info("Skipping duplicate {} — stored amount {} >= incoming {}", transactionId, existing.get().getAmount(), amount);
                    return;
                }
                targetRepository.delete(existing.get());
                log.info("Replacing duplicate {} with higher amount {}", transactionId, amount);
            }

            TransactionTarget target = new TransactionTarget();
            target.setTransactionId(transactionId);
            target.setAccountId(accountId);
            target.setAmount(amount);
            target.setCurrency(currency);
            target.setTimestamp(parseDate(timestamp));
            target.setStatus(status);
            target.setDescription(description);
            targetRepository.save(target);
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
