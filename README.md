# Wallet Ledger API

> **Release status:** portfolio project. Synthetic data only. Not audited, not deployed, not for real funds.
> Uses production-style patterns; it is not production-deployed.

Work in progress (WP 1: foundation). The full README (architecture diagram, design decisions,
benchmarks, Limitations, Hardening before production) is written in WP 9.

## Quickstart

Requires Docker and, for local runs and tests, Java 21 and Maven.

```bash
# Start Postgres + the app
docker compose up --build

# Health check
curl http://localhost:8080/actuator/health
```

Run the tests (needs a running Docker daemon, because Testcontainers starts a real Postgres):

```bash
mvn verify
```

## Status

| Work package | State |
|---|---|
| WP 1 Foundation | In progress |
| WP 2 Auth and accounts | Not started |
