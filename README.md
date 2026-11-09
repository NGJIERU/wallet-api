# Wallet Ledger API

> **Release status:** portfolio project. Synthetic data only. Not audited, not deployed, not for real funds.
> Uses production-style patterns; it is not production-deployed.

Work in progress. The full README (architecture diagram, design decisions, benchmarks,
Limitations, Hardening before production) is written in WP 9.

## Requirements

| Tool | Version | Notes |
|---|---|---|
| Java | **21 exactly** | The Maven build fails fast on any other major version (maven-enforcer rule) |
| Maven | 3.9 | Same major version the Dockerfile uses |
| Docker | Docker Desktop, engine 28.x | Runs the stack and the Testcontainers tests |
| Kafka | `apache/kafka:4.2.2` (single-node KRaft) | Same tag in `docker-compose.yml` and the tests; matches the `kafka-clients` 4.2.2 managed by Boot 4.1.2. Multi-arch (arm64 and amd64) |
| Spring Boot | 4.1.x (built and tested on 4.1.2) | Managed by the parent POM |

Java 21 setup on macOS:

```bash
brew install --cask temurin@21
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # add this line to ~/.zshrc to make it permanent
java -version                                       # must report 21
```

On other platforms, install any JDK 21 distribution and point `JAVA_HOME` at it.

## Quickstart

```bash
# Start Postgres, Kafka and the app
docker compose up --build

# Health check (compose maps the app to host port 8081)
curl http://localhost:8081/actuator/health

# The events topic is declared by the app at start-up. Describe it from inside the broker
# (expect PartitionCount 3, ReplicationFactor 1).
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic wallet.transaction-completed.v1

# Watch the published events (key = account id, headers event-id and event-type). Ctrl+C to stop.
# Notifications written by the consumer (one row per event):
# docker compose exec postgres psql -U wallet -d wallet -c "select user_id, direction, amount, message from notifications order by id desc limit 10"

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic wallet.transaction-completed.v1 --from-beginning --formatter-property print.key=true --formatter-property print.headers=true
```

The outbox relay publishes unpublished rows in id order about every 500 ms. Properties: `app.outbox.relay.enabled` (true in the app, false in tests), `batch-size` (100), `poll-interval` (500ms), `backoff-min` (1s), `backoff-max` (30s), `send-timeout-ms` (12000) and `app.outbox.topic`.

Try the API (the compose file supplies a throwaway dev `JWT_SECRET`):

```bash
curl -X POST http://localhost:8081/auth/register -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"password123"}'

TOKEN=$(curl -s -X POST http://localhost:8081/auth/login -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"password123"}' | sed -E 's/.*"accessToken":"([^"]+)".*/\1/')

curl -X POST http://localhost:8081/accounts -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"currency":"SGD"}'
curl http://localhost:8081/accounts -H "Authorization: Bearer $TOKEN"
```

Move money. Every POST needs an `Idempotency-Key` header (1 to 64 printable ASCII characters, no spaces).
Amounts are in minor units (2500 = 25.00). Replace the account ids with your own; a transfer needs
two accounts in the same currency.

```bash
curl -X POST http://localhost:8081/accounts/1/deposits -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" -d '{"amount":2500}'

curl -X POST http://localhost:8081/accounts/1/withdrawals -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: demo-2" -H "Content-Type: application/json" -d '{"amount":500}'

curl -X POST http://localhost:8081/transfers -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: demo-3" -H "Content-Type: application/json" \
  -d '{"fromAccountId":1,"toAccountId":2,"amount":1000}'

# History is newest first. Pass the returned cursor to get the next page.
curl "http://localhost:8081/accounts/1/transactions?limit=20" -H "Authorization: Bearer $TOKEN"
```

Sending the same request again with the same `Idempotency-Key` returns the stored response and moves no money.
The same key with a different body returns 422.

Run the tests (needs a running Docker daemon, because Testcontainers starts a real Postgres):

```bash
mvn verify
```

Run only the concurrency tests (tagged `concurrency`, see the next section):

```bash
mvn test -Dgroups=concurrency
```

## Concurrency proof (WP 4)

Production-style patterns, proven under real parallel load against a real PostgreSQL 16 (Testcontainers). The tests call the services directly (no HTTP), with up to 50 threads started together on a latch.

### What is tested

| Scenario | Result |
|---|---|
| Random transfers: 8 accounts x 10,000, 16 threads x 25 transfers | 400 of 400 succeed, total balance stays 80,000, ledger rows = 8 + 2 x 400, no negative balance |
| Hot account: 50 withdrawals of 100 on a balance of 1,000 | exactly 10 succeed, 40 refused (insufficient funds), final balance 0 |
| Hot account: 25 deposits and 25 withdrawals on one account, shuffled | every deposit succeeds, balance equals the exact sum of successful operations, never negative |
| One source to 20 destinations | all 20 succeed, money conserved |
| Same idempotency key, same body, 20 threads | 20 identical 201 responses, 1 transaction, 2 ledger entries, debited once |
| Same key, 20 different amounts | exactly 1 succeeds, 19 rejected as key reuse, only the winner's amount debited |
| Same key, first attempt fails (insufficient funds) | all 20 refused, no key stored, nothing changed |
| Real deadlock (forced, opposite lock order) | Postgres kills one transaction (40P01), the retry wrapper re-runs it once, both transfers applied exactly once |

Every test also asserts conservation (total balance) and exact row counts. The reconciliation check (SUM(CREDIT) - SUM(DEBIT) = balance) runs after every integration test, but it cannot catch a double-applied transfer, so it is never the only check.

### How it is checked

- Outcomes are classified by exception type (`OK`, `INSUFFICIENT`, `KEY_REUSE`, `BUSY`, `UNEXPECTED`). Any `UNEXPECTED` outcome fails the test.
- No sleeps are used for synchronization. A hang fails after 60 s instead of freezing the build.
- Random plans use a seeded `Random` and the seed is logged, so a failing run can be replayed.
- The concurrency tests share one Spring context (lock timeout 30 s, connection pool 50) and a Postgres container with `max_connections=200`.

### Results

Needs JDK 21 and Docker. Measured locally (Apple M2 Pro, JDK 21.0.12.1): on the Spring Boot 3.4.1 tree the 13 concurrency tests took 9.4 to 11.3 s per run, 10 of 10 runs green. On 4.1.2 the tag has 18 tests (13 + 5 outbox): 3 of 3 runs green after step 5.1 (Maven total 12.9 to 36.4 s) and 3 of 3 after step 5.2 (14.5 to 42.3 s). Times vary between runs for reasons not isolated, so they are not benchmarks. CI (GitHub Actions, ubuntu-24.04): the full suite (121 tests) is green in about 1m15s (run #30, 1m16s in total).

### Limitations

- Deposits and withdrawals have no deadlock retry. A deadlock there is unreachable by design (claim first, one row lock). Add a retry if a second lock is ever added.
- A busy account returns 409 after the lock timeout (2 s by default). Clients must retry.
- Tests run in one JVM against one database. They do not cover multiple app instances or HTTP-level load (that is WP 8).
- The tests prove exact outcomes. They do not measure how long threads waited on row locks.

## Benchmark environment

Benchmark numbers are published in WP 8. They are only meaningful with the environment beside them,
so the fields below are filled in at benchmark time, not guessed.

| Item | Value |
|---|---|
| Hardware | Apple M2 Pro, 12 cores (8 performance, 4 efficiency), 32 GB RAM |
| OS | macOS 15.3.2 |
| JDK (vendor and exact version) | _fill from `java -version` at benchmark time_ |
| Docker engine | 28.4.0 (Docker Desktop) |
| Docker Desktop VM memory | 16 GB (raised on 4 Nov 2026 from the 7.8 GB default) |

Docker on macOS runs inside a Linux VM, so these numbers are not equivalent to bare-metal Linux.
They are a reproducible baseline for this project, not production capacity figures.

## Status

| Work package | State |
|---|---|
| WP 1 Foundation | Done. Build, tests and CI green |
| WP 2 Auth and accounts | Done. Register, login (JWT), account endpoints with ownership checks, 17 tests, CI green |
| WP 3 Money movement | Done (M2). Idempotent deposit, withdrawal and transfer, cursor-paginated history, and a ledger reconciliation check after every integration test. 92 tests, 0 failures with `mvn verify`. CI run: #18 green, 57s |
| WP 4 Concurrency proof | Done (M3 gate: GO). Random transfers, hot account, idempotency-key races and a retry on a real deadlock, 13 tests tagged `concurrency`, no production code changed. 105 tests, 0 failures with `mvn verify`. CI run: #21 green, 1m17s |
| WP 5 step 5.0 Boot 4.1.2 migration | Done. Same behaviour and the same 105 tests, 0 failures with mvn verify. CI run: #23 green, 1m6s |
| WP 5 step 5.1 Outbox write path | Done. One outbox event per ledger entry, written inside the money transaction (an outbox failure rolls the money movement back). 119 tests, 0 failures with `mvn verify`; the `concurrency` tag now has 18 tests. CI run: #27 green, 1m14s |
| WP 5 step 5.2 Kafka infrastructure | Done. Single-node KRaft broker (`apache/kafka:4.2.2`) in Compose, the app declares topic `wallet.transaction-completed.v1` (3 partitions, replication factor 1), shared Testcontainers broker for the tests. Kafka is not part of the health groups; with the broker stopped, health stays UP and deposits still succeed. 121 tests, 0 failures with `mvn verify`. CI run: #30 green, 1m16s |
| WP 5 step 5.3 Outbox relay | Done. A background relay publishes outbox rows to Kafka in id order and marks a row only after the broker acknowledges it (at-least-once). Survives a broker outage and a deleted topic without restarting the app. 133 tests, 0 failures with `mvn verify`; the `concurrency` tag has 19 tests, 3 of 3 green. CI run: #32 green, 1m39s |
| WP 5 step 5.4 Idempotent consumer | Done. A Kafka consumer writes one notification per event (Flyway V2 `notifications`), deduplicating on `processed_events` in the same database transaction, with the offset committed after the DB commit. Poison and exhausted-retry records go to a dead-letter topic (`wallet.transaction-completed.v1-dlt`). A broker outage at start-up does not stop the app. 155 tests, 0 failures with `mvn verify`; the `concurrency` tag has 20 tests, 3 of 3 green. CI run: #33 green, 1m49s |
| WP 5 step 5.5 Failure proof | Done. Tests for the consumer lifecycle (stubbed registry) and for the whole chain against a hung broker (money endpoints unaffected, everything drains after the unpause with no restart). The `concurrency` tag passed 10 of 10 runs with zero ordering violations and `mvn verify` 3 of 3. A Docker chaos run (SIGKILL and SIGTERM mid-drain, broker paused, broker stopped, topic deleted, forced duplicates) ended with ledger entries = outbox events = notifications and no duplicate notification. 159 tests, 0 failures with `mvn verify`. CI run: #35 green, 2m2s |

## Outbox, relay and consumer (WP 5)

Production-style pattern: every ledger entry writes one row to `outbox_events` in the same database transaction as the money movement. A deposit or withdrawal writes 1 event, a transfer writes 2 (debit for the source account, credit for the destination). A background relay publishes these rows to Kafka in id order, and a consumer turns each event into exactly one notification (see the status table above and the limitations below).

Limitations (so far):

- The outbox table is never purged and grows without bound. Fix before production: a retention job or partitioning.
- Delivery will be at-least-once, so consumers must dedupe.
- One relay instance is planned. Several relays could break per-account order.
- The deadlock-plus-retry outbox test forces its own deadlock, so it does not exercise the retry path of the real money services (that path is unreachable by design).
- Delivery is at-least-once, so the consumer dedupes: `processed_events` and the `notifications` row commit in one transaction, and `notifications.event_id` is UNIQUE as a second defence.
- `processed_events` and `notifications` are never purged.
- A permanently failing record is dead-lettered, but there is no tool to replay the dead-letter topic. Dead-letter records carry a full stack trace in a header (Spring default).
- The dead-letter topic is declared at app start-up only: if it is deleted while the app runs, it is recreated at the next start.
- One consumer thread, one consumer group; no REST endpoint to read notifications.
- The relay is a single instance. Several relays would double-publish and could reorder an account's events.
- Head-of-line blocking: a row that can never be published blocks the relay (no quarantine or dead-letter topic yet).
- Delivery is at-least-once, so a crash between publish and mark gives a duplicate. Consumers must dedupe on the `event-id` header.
- The relay polls the table. Hardening path: change-data-capture (Debezium) or LISTEN/NOTIFY.
- Deleting the topic by hand loses records already marked published.
- Spring's producer listener logs the failed record's payload at ERROR level during a broker outage. Acceptable with synthetic data; disable or redact before production.
- Kafka is a single broker with replication factor 1 and no data volume in Compose. Fix before production: a multi-broker cluster with replication factor 3 and `min.insync.replicas` 2.
- The app declares its topic once, at start-up, and does not retry. If it starts while the broker is down, or the broker loses its data, the topic is missing until the next app restart (broker auto-creation is off on purpose).
- With the broker down the app logs an ERROR from `KafkaAdmin` at start-up and carries on. With an unreachable broker address, start-up takes about 10 s instead of about 3 s (admin timeouts shortened from a measured 43 s).
- A notification for a user that does not exist (a foreign-key failure) is retried 4 times and then dead-lettered.
- If the app starts while the broker is down, the dead-letter topic is not re-declared when the broker returns (the relay re-declares only the main topic). A poison record arriving before the next app restart has no dead-letter topic to go to.
- Graceful shutdown can interrupt the relay between the broker acknowledgement and the mark, which produces one duplicate on the topic (seen once in the chaos run). The consumer absorbed it.

What the failure testing proved: with a hung, stopped or deleted broker the money endpoints kept returning 201 and health stayed UP; after the broker returned, every outbox row was published and every event produced exactly one notification without restarting the app; forced and shutdown-induced duplicates on the topic produced no duplicate notification. What it did not prove: that a `kill -9` produces a duplicate (the one kill did not), replay of records lost with a deleted topic, a long outage, several consumers or relays, other crash points on the consumer side, or any throughput figure. The chaos run is a correctness check, not a benchmark.
