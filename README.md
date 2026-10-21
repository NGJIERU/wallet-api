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
| WP 3 Money movement | Done (M2). Idempotent deposit, withdrawal and transfer, cursor-paginated history, and a ledger reconciliation check after every integration test. 92 tests, 0 failures with `mvn verify`. CI run: _record after push_ |