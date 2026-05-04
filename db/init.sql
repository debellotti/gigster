-- PostgreSQL initialization script for Gig Migration Challenge
-- Creates schema and initial tables

CREATE SCHEMA IF NOT EXISTS gig;

-- Transactions source table
CREATE TABLE gig.transactions_source (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    transaction_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions target table (after processing)
CREATE TABLE gig.transactions_target (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    status VARCHAR(50) NOT NULL,
    description TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_stage VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_source_transaction_id ON gig.transactions_source(transaction_id);
CREATE INDEX idx_source_user_id ON gig.transactions_source(user_id);
CREATE INDEX idx_source_status ON gig.transactions_source(status);

CREATE INDEX idx_target_transaction_id ON gig.transactions_target(transaction_id);
CREATE INDEX idx_target_user_id ON gig.transactions_target(user_id);
CREATE INDEX idx_target_status ON gig.transactions_target(status);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA gig TO gig_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gig TO gig_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gig TO gig_user;
