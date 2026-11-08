-- V2: notifications written by the idempotent consumer (one row per outbox event).
-- No foreign keys to accounts or transactions: the consumer is conceptually a separate context.
-- user_id references users so the recipient must exist.
CREATE TABLE notifications (
    id               BIGSERIAL    PRIMARY KEY,
    event_id         BIGINT       NOT NULL UNIQUE,   -- = outbox_events.id; second line of defence behind processed_events
    user_id          BIGINT       NOT NULL REFERENCES users (id),
    account_id       BIGINT       NOT NULL,
    transaction_id   UUID         NOT NULL,
    ledger_entry_id  BIGINT       NOT NULL,
    transaction_type VARCHAR(16)  NOT NULL CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
    direction        VARCHAR(6)   NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount           BIGINT       NOT NULL CHECK (amount > 0),
    currency         VARCHAR(3)   NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    message          VARCHAR(255) NOT NULL,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_notifications_user_id_id ON notifications (user_id, id DESC);
