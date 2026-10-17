-- V1: core schema for the wallet ledger.
-- Money is BIGINT in minor units (sen/cents). Never floating point.
-- Currency is stored as VARCHAR(3) (not CHAR(3)) so Hibernate schema validation maps it cleanly.

CREATE TABLE users (
    id            BIGSERIAL    PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id         BIGSERIAL   PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    currency   VARCHAR(3)  NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance    BIGINT      NOT NULL DEFAULT 0 CHECK (balance >= 0),   -- second line of defense; service validates first
    version    BIGINT      NOT NULL DEFAULT 0,                        -- reserved for the optional locking benchmark
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, currency)                                        -- one account per currency per user
);

CREATE TABLE transactions (
    id         UUID        PRIMARY KEY,
    type       VARCHAR(16) NOT NULL CHECK (type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
    currency   VARCHAR(3)  NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount     BIGINT      NOT NULL CHECK (amount > 0),
    created_by BIGINT      NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only: the application never UPDATEs or DELETEs rows here.
-- Deposits/withdrawals are single-sided entries (documented limitation vs. full double-entry).
-- Reconciliation invariant: SUM(CREDIT) - SUM(DEBIT) == accounts.balance for every account.
CREATE TABLE ledger_entries (
    id             BIGSERIAL   PRIMARY KEY,
    transaction_id UUID        NOT NULL REFERENCES transactions (id),
    account_id     BIGINT      NOT NULL REFERENCES accounts (id),
    direction      VARCHAR(6)  NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_ledger_account_id_id ON ledger_entries (account_id, id DESC);  -- cursor pagination

-- A row exists only if the money transaction committed (the claim runs inside it),
-- so there is no IN_PROGRESS state: response columns are NOT NULL.
CREATE TABLE idempotency_keys (
    user_id       BIGINT      NOT NULL REFERENCES users (id),
    key           VARCHAR(64) NOT NULL,
    request_hash  VARCHAR(64) NOT NULL,   -- SHA-256 hex of method + path + body
    response_code INT         NOT NULL,
    response_body JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);

CREATE TABLE outbox_events (
    id           BIGSERIAL   PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,    -- account id, used as the Kafka key
    type         VARCHAR(64) NOT NULL,    -- e.g. 'TransactionCompleted.v1'
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_events (id) WHERE published_at IS NULL;

-- Consumer-side dedupe for at-least-once delivery.
CREATE TABLE processed_events (
    consumer_group VARCHAR(64) NOT NULL,
    event_id       BIGINT      NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
