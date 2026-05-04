package com.gig.repository;

import com.gig.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Transaction Repository - Data Access Layer
 */
@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {

    /**
     * Placeholder: Find transaction by ID
     */
    Optional<Transaction> findByTransactionId(String transactionId);

    /**
     * Placeholder: Find transactions by user
     */
    // TODO: Implement query methods
}
