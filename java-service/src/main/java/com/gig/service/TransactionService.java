package com.gig.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gig.kafka.TransactionProducer;
import com.gig.model.Transaction;
import com.gig.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private TransactionProducer transactionProducer;

    @Autowired
    private ObjectMapper objectMapper;

    public List<Transaction> getAllTransactions() {
        return transactionRepository.findAll();
    }

    public Optional<Transaction> getTransaction(String transactionId) {
        return transactionRepository.findByTransactionId(transactionId);
    }

    public Transaction createTransaction(Transaction tx) {
        Transaction saved = transactionRepository.save(tx);
        try {
            transactionProducer.sendTransaction(objectMapper.writeValueAsString(toKafkaMap(tx)));
        } catch (Exception e) {
            log.warn("Failed to publish transaction to Kafka: {}", e.getMessage());
        }
        return saved;
    }

    public int loadFromCsv(String filePath) throws Exception {
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] fields = line.split(",", -1);
                if (fields.length < 6) continue;

                Map<String, Object> msg = new LinkedHashMap<>();
                msg.put("transaction_id", fields[0].trim());
                msg.put("account_id", fields[1].trim());
                msg.put("amount", fields[2].trim());
                msg.put("currency", fields[3].trim());
                msg.put("timestamp", fields[4].trim());
                msg.put("status", fields[5].trim());
                if (fields.length > 6) msg.put("description", fields[6].trim());

                transactionProducer.sendTransaction(objectMapper.writeValueAsString(msg));
                count++;
            }
        }
        log.info("Loaded {} transactions from CSV: {}", count, filePath);
        return count;
    }

    private Map<String, Object> toKafkaMap(Transaction tx) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("transaction_id", tx.getTransactionId());
        msg.put("account_id", tx.getAccountId());
        msg.put("amount", tx.getAmount().toPlainString());
        msg.put("currency", tx.getCurrency());
        msg.put("timestamp", tx.getTimestamp().format(DT_FMT));
        msg.put("status", tx.getStatus());
        if (tx.getDescription() != null) msg.put("description", tx.getDescription());
        return msg;
    }
}
