package com.gig.repository;

import com.gig.model.TransactionTarget;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TransactionTargetRepository extends JpaRepository<TransactionTarget, Long> {
    Optional<TransactionTarget> findByTransactionId(String transactionId);
}
