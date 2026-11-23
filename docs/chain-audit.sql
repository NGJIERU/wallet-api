-- Chain audit queries (read-only). Run after any load or failure test, for example:
--   docker compose exec -T postgres psql -U wallet -d wallet -f - < docs/chain-audit.sql
-- Expected after the system has drained: broken accounts = 0, unpublished = 0, duplicate notification event ids = 0,
-- and ledger = outbox = notifications = processed_events. Event counts are cumulative since the volume was created.
-- If the run injected forged or poison records, they create no ledger row or notification, so the equality still holds.

-- 1. Reconciliation: accounts whose balance differs from SUM(CREDIT) - SUM(DEBIT). Expect 0 rows.
SELECT a.id AS broken_account, a.balance,
       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0) AS ledger_sum
FROM accounts a LEFT JOIN ledger_entries l ON l.account_id = a.id
GROUP BY a.id, a.balance
HAVING a.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0);

-- 2. Counts: ledger entries = outbox events = notifications = processed events.
SELECT (SELECT count(*) FROM ledger_entries)  AS ledger,
       (SELECT count(*) FROM outbox_events)   AS outbox,
       (SELECT count(*) FROM notifications)   AS notifications,
       (SELECT count(*) FROM processed_events) AS processed_events;

-- 3. Unpublished outbox rows. Expect 0 once the relay has drained.
SELECT count(*) AS unpublished FROM outbox_events WHERE published_at IS NULL;

-- 4. Duplicate notifications for one event id. Expect 0.
SELECT count(*) AS duplicate_event_ids
FROM (SELECT event_id FROM notifications GROUP BY event_id HAVING count(*) > 1) d;
