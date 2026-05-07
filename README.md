# GiG Migration Challenge

A three-phase data migration pipeline: ingest legacy financial data (CSV), stream it through a message broker, persist it via a microservice, and validate data integrity end-to-end.

## Architecture

```
data/transactions.csv
  └── Apache NiFi (Phase 2)
        GetFile → SplitText → RouteOnContent (skip header)
                            → RouteOnContent (detect malformed)
                                ├── malformed → LogMessage (bulletin)
                                └── valid → ReplaceText (CSV→JSON)
                                              → Kafka: transactions-topic
                                                    └── TransactionConsumer
                                                          └── PostgreSQL: transactions_migrated
  └── Python reconciliation (Phase 3)
```

---

## Phases

### Phase 1 — Java Microservice (`java-service/`)

Spring Boot service that manages financial accounts and persists transaction history.

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/transactions/health` | Health check |
| GET | `/api/accounts` | List all accounts |
| GET | `/api/accounts/{accountId}` | Get account by ID |
| POST | `/api/accounts` | Create an account |
| POST | `/api/accounts/{accountId}/transfers` | Transfer money between accounts |
| GET | `/api/transactions` | List all migrated transactions |
| GET | `/api/transactions/{id}` | Get transaction by ID |
| POST | `/api/transactions/load-csv` | Manually trigger CSV ingestion (fallback) |

### Phase 2 — Apache NiFi (`nifi/conf/flow.json.gz`)

The NiFi flow is committed as `flow.json.gz` and loaded automatically when the NiFi container starts — no manual setup step required. The flow was originally built via `scripts/setup-nifi-flow.py` (kept for reference), and its output is the committed `flow.json.gz`. The flow runs 11 processors:

1. **GetFile** — reads `transactions.csv` from `/app/data/` (not `GenerateFlowFile`)
2. **SplitText** — one flowfile per CSV line
3. **RouteOnContent** — skips the header line
4. **RouteOnContent** — detects malformed rows (empty fields, `INVALID_AMT`, `ERR` currency)
5. **ReplaceText** — converts a valid CSV line to a JSON object
6. **UpdateAttribute + RouteOnAttribute** — retry loop for ReplaceText failures (up to 3 attempts before LogMessage)
7. **PublishKafka** — publishes to `transactions-topic`
8. **UpdateAttribute + RouteOnAttribute** — retry loop for PublishKafka failures (up to 3 attempts before LogMessage)
9. **LogMessage** — logs all skipped/malformed/failed rows to the NiFi bulletin board

Error-handling topology:

```
ReplaceText  --(failure)--> UpdateAttribute (transform.retry.count+1)
                                 --> RouteOnAttribute
                                       ├── retry  (count < 3) --> ReplaceText
                                       └── unmatched (count ≥ 3) --> LogMessage

PublishKafka --(failure)--> UpdateAttribute (kafka.retry.count+1)
                                 --> RouteOnAttribute
                                       ├── retry  (count < 3) --> PublishKafka
                                       └── unmatched (count ≥ 3) --> LogMessage
```

### Phase 3 — Reconciliation (`scripts/reconciliation.py`)

Reads the original CSV and the database to produce a report:

```
Total Source Records    : 10
Successfully Migrated   : 6
Failed / Skipped        : 2  (1 invalid amount, 1 missing fields)
Duplicate rows resolved : 2  (kept highest amount)
Total Financial Value   : per-currency sum of persisted amounts
Overall result          : PASS
```

---

## Design Decisions

### Duplicate Handling

The CSV contains three rows with the same `transaction_id` (`550e8400-...`) with amounts 150.00, 200.00, and 120.00.

**Strategy: keep the record with the highest amount.**

This is implemented in `TransactionConsumer.java`: when a message arrives for an already-persisted `transaction_id`, the incoming amount is compared to the stored one. If the new amount is higher, the existing record is deleted and replaced. Otherwise the incoming message is discarded. This logic runs in the Java consumer after messages pass through NiFi, so it is independent of the ingestion path.

### Malformed Data

The CSV contains two intentionally broken rows:
- Row with `INVALID_AMT` as the amount
- Row with empty `transaction_id` and `account_id`

**Strategy: detect early in NiFi, log and discard — never send to Kafka.**

NiFi's `RouteOnContent` processor matches against a regex (`INVALID_AMT|^,|,,|,ERR,`) before any downstream processing. Malformed rows are routed directly to `LogMessage`. For rows that pass validation but fail transformation or publishing, the retry loops attempt recovery up to 3 times before routing to `LogMessage` as permanent failure. No malformed or unrecoverable row ever reaches the database.

### Currency and Amount Storage

Amounts are stored as `DECIMAL(18, 2)` in PostgreSQL. This avoids floating-point precision errors that are unacceptable in financial data. Currencies are kept as-received (EUR, GBP, USD) — no normalization is applied, since the challenge prohibits altering source data.

### Why Kafka Between NiFi and Java

Kafka decouples the ingestion speed (NiFi) from the persistence speed (Java). If the Java service is slow or temporarily down, messages queue in Kafka without blocking NiFi. It also makes the consumer independently replayable.

### NiFi Flow as Code

The NiFi flow is committed as `nifi/conf/flow.json.gz` and loaded automatically by the NiFi container on start — no separate setup script or service is needed. The flow was originally generated by `scripts/setup-nifi-flow.py` (which calls the NiFi REST API to build the 11-processor chain). Committing the resulting `flow.json.gz` makes the configuration reproducible on any clean environment with zero extra steps: `docker-compose up -d --build` is sufficient.

---

## Running from scratch

### Prerequisites

- Docker & Docker Compose
- Python 3 with `pip install psycopg2-binary` (reconciliation only)
- Ports `8080`, `5432`, `9092`, `2181`, `8161` free

### Step 1 — Start the full stack

```bash
docker-compose up -d --build
```

This single command provisions all four components automatically:
1. **PostgreSQL** — initialised with the schema
2. **Kafka + Zookeeper** — message broker
3. **NiFi** — starts with `flow.json.gz` pre-loaded from `nifi/conf/`; the flow is active immediately, no separate setup step
4. **Java service** — built as a FAT-Jar and started

Follow NiFi logs:

```bash
docker-compose logs -f nifi
```

### Step 2 — Wait for the Java service

```bash
until curl -s http://localhost:8080/api/transactions/health | grep -q running; do sleep 3; done
```

View the NiFi flow at `https://localhost:8161/nifi` — username: `admin`, password: `admin123456789` (accept the self-signed certificate).

### Step 3 — Verify the API

```bash
chmod +x scripts/verify-api.sh
./scripts/verify-api.sh
```

This demonstrates account creation, money transfers, balance validation, and rejection of insufficient-balance transfers.

### Step 4 — Run reconciliation

NiFi reads the CSV and pushes rows to Kafka within seconds of the flow starting. Wait ~10 seconds after the Java service is up, then:

```bash
python3 scripts/reconciliation.py
```

Expected output:

```
Total Source Records    : 10
Successfully Migrated   : 6
Failed / Skipped        : 2
Duplicate rows resolved : 2
Overall result          : PASS
```

---

## Reset and retest from scratch

```bash
# 1. Tear down containers and DB volumes
docker-compose down -v

# 2. Start everything (NiFi flow configured automatically)
docker-compose up -d --build

# 3. Wait for Java service
until curl -s http://localhost:8080/api/transactions/health | grep -q running; do sleep 3; done

# 4. Verify
./scripts/verify-api.sh
sleep 10
python3 scripts/reconciliation.py
```

---

## Useful commands

```bash
# Follow Java service logs
docker-compose logs -f java-service

# Check record count in Postgres
docker exec gig-postgres psql -U gig_user -d gig_db \
  -c "SELECT COUNT(*) FROM gig.transactions_migrated;"

# List Kafka topics
docker exec gig-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## Project structure

```
├── docker-compose.yml
├── data/
│   └── transactions.csv              # source dataset (10 rows, intentionally dirty)
├── db/
│   └── init.sql                      # schema: accounts, transactions_migrated
├── java-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/gig/
│       ├── controller/               # AccountController, TransactionController
│       ├── kafka/                    # TransactionProducer, TransactionConsumer
│       ├── model/                    # Account, Transaction
│       ├── repository/               # JPA repos
│       └── service/                  # AccountService, TransactionService
├── scripts/
│   ├── setup-nifi-flow.py            # utility: regenerates flow.json.gz via NiFi REST API
│   ├── reconciliation.py             # Phase 3 validation report
│   └── verify-api.sh                 # Phase 1 API smoke tests
└── nifi/
    └── conf/                         # NiFi config and pre-loaded flow.json.gz
```

## Tech stack

| Component | Technology |
|-----------|-----------|
| REST API + microservice | Java 17 / Spring Boot 3 |
| Ingestion & orchestration | Apache NiFi 2.9 |
| Messaging | Apache Kafka 3 |
| Database | PostgreSQL 15 |
| Reconciliation | Python 3 |
| Infrastructure | Docker Compose |
