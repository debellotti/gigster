-- PostgreSQL initialization script for Gig Migration Challenge

CREATE SCHEMA IF NOT EXISTS gig;

-- Accounts table (managed via REST API)
CREATE TABLE gig.accounts (
    id            BIGSERIAL PRIMARY KEY,
    account_id    VARCHAR(255) UNIQUE NOT NULL,
    owner_name    VARCHAR(255) NOT NULL,
    balance       DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    currency      VARCHAR(3) NOT NULL DEFAULT 'EUR',
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions source table (populated by Kafka consumer from NiFi pipeline)
CREATE TABLE gig.transactions_source (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    account_id     VARCHAR(255) NOT NULL,
    amount         DECIMAL(18, 2) NOT NULL,
    currency       VARCHAR(3) NOT NULL DEFAULT 'EUR',
    timestamp      TIMESTAMP NOT NULL,
    status         VARCHAR(50) NOT NULL DEFAULT 'SUCCESS',
    description    TEXT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transactions target table (populated by NiFiTransformerService)
CREATE TABLE gig.transactions_target (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) UNIQUE NOT NULL,
    account_id     VARCHAR(255) NOT NULL,
    amount         DECIMAL(18, 2) NOT NULL,
    currency       VARCHAR(3) NOT NULL,
    timestamp      TIMESTAMP NOT NULL,
    status         VARCHAR(50) NOT NULL,
    description    TEXT,
    processed_by   VARCHAR(50) DEFAULT 'NiFi-Simulator',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_source_transaction_id ON gig.transactions_source(transaction_id);
CREATE INDEX idx_source_account_id     ON gig.transactions_source(account_id);
CREATE INDEX idx_target_transaction_id ON gig.transactions_target(transaction_id);
CREATE INDEX idx_target_account_id     ON gig.transactions_target(account_id);
CREATE INDEX idx_accounts_account_id   ON gig.accounts(account_id);

GRANT ALL PRIVILEGES ON SCHEMA gig TO gig_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gig TO gig_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gig TO gig_user;
