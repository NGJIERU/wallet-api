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

Java 21 setup on macOS:

```bash
brew install --cask temurin@21
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # add this line to ~/.zshrc to make it permanent
java -version                                       # must report 21
```

On other platforms, install any JDK 21 distribution and point `JAVA_HOME` at it.

## Quickstart

```bash
# Start Postgres + the app
docker compose up --build

# Health check (compose maps the app to host port 8081)
curl http://localhost:8081/actuator/health
```

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

Needs JDK 21 and Docker. Measured locally (Apple M2 Pro, JDK 21.0.12.1): the 13 concurrency tests took 9.4 to 11.3 s per run, 10 of 10 runs green. CI (GitHub Actions, ubuntu-24.04): the full suite of 105 tests is green in about 1 minute (run #21, 1m17s in total).

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
| Docker Desktop VM memory | _fill at benchmark time (7.8 GB by default; raise before the Kafka work)_ |

Docker on macOS runs inside a Linux VM, so these numbers are not equivalent to bare-metal Linux.
They are a reproducible baseline for this project, not production capacity figures.

## Status

| Work package | State |
|---|---|
| WP 1 Foundation | Done. Build, tests and CI green |
| WP 2 Auth and accounts | Done. Register, login (JWT), account endpoints with ownership checks, 17 tests, CI green |
| WP 3 Money movement | Done (M2). Idempotent deposit, withdrawal and transfer, cursor-paginated history, and a ledger reconciliation check after every integration test. 92 tests, 0 failures with `mvn verify`. CI run: #18 green, 57s |
| WP 4 Concurrency proof | Done (M3 gate: GO). Random transfers, hot account, idempotency-key races and a retry on a real deadlock, 13 tests tagged `concurrency`, no production code changed. 105 tests, 0 failures with `mvn verify`. CI run: #21 green, 1m17s |