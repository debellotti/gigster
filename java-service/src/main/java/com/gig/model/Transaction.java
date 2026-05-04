package com.gig.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Transaction Entity Model
 */
@Entity
@Table(name = "transactions_source", schema = "gig")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String transactionId;

    @Column(nullable = false)
    private String userId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String currency;

    @Column(nullable = false)
    private LocalDateTime transactionDate;

    @Column(nullable = false)
    private String status;

    private String description;

    @Column(updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    // Constructors
    public Transaction() {}

    public Transaction(String transactionId, String userId, BigDecimal amount,
                       String currency, LocalDateTime transactionDate, String status) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.transactionDate = transactionDate;
        this.status = status;
    }

    // Getters
    public Long getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public String getUserId() { return userId; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public LocalDateTime getTransactionDate() { return transactionDate; }
    public String getStatus() { return status; }
    public String getDescription() { return description; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }

    // Setters
    public void setId(Long id) { this.id = id; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setCurrency(String currency) { this.currency = currency; }
    public void setTransactionDate(LocalDateTime transactionDate) { this.transactionDate = transactionDate; }
    public void setStatus(String status) { this.status = status; }
    public void setDescription(String description) { this.description = description; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}
