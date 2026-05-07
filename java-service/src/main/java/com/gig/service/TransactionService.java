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
            reader.readLine(); // skip header: transaction_id,user_id,amount,currency,transaction_date,status,description
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] f = line.split(",", -1);
                if (f.length < 6) continue;

                Map<String, Object> msg = new LinkedHashMap<>();
                msg.put("transaction_id", f[0].trim());
                msg.put("user_id", f[1].trim());
                msg.put("amount", f[2].trim());
                msg.put("currency", f[3].trim());
                msg.put("transaction_date", f[4].trim());
                msg.put("status", f[5].trim());
                if (f.length > 6) msg.put("description", f[6].trim());

                transactionProducer.sendTransaction(objectMapper.writeValueAsString(msg));
                count++;
                log.info("Published CSV row {} to Kafka", f[0].trim());
            }
        }
        log.info("Loaded {} transactions from CSV: {}", count, filePath);
        return count;
    }

    private Map<String, Object> toKafkaMap(Transaction tx) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("transaction_id", tx.getTransactionId());
        msg.put("user_id", tx.getUserId());
        msg.put("amount", tx.getAmount().toPlainString());
        msg.put("currency", tx.getCurrency());
        msg.put("transaction_date", tx.getTransactionDate().format(DT_FMT));
        msg.put("status", tx.getStatus());
        if (tx.getDescription() != null) msg.put("description", tx.getDescription());
        return msg;
    }
}
