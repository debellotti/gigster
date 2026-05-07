# Gig Migration Challenge

A three-phase data migration pipeline that ingests transaction data, processes it through a transformation layer, validates results, and ensures data integrity.

## Architecture

```
data/transactions.csv
  → POST /api/transactions/load-csv
  → Kafka: transactions-topic          (raw: user_id, transaction_date)
      ├── Apache NiFi (Phase 2)
      │     ConsumeKafka → JoltTransformJSON → PublishKafka
      │     (renames user_id→account_id, transaction_date→timestamp)
      │     → Kafka: transactions-processed
      │           → Java TransactionConsumer
      │           └── PostgreSQL: transactions_source
      └── Java NiFiTransformerService
            └── PostgreSQL: transactions_target
  → Python reconciliation
```

## Phases

**Phase 1 — Java Service** (`java-service/`)
Spring Boot service that exposes REST endpoints, reads the CSV file, and publishes each row to `transactions-topic`. It also consumes the processed events from `transactions-processed` to persist them in `transactions_source`.

**Phase 2 — Apache NiFi** (configured via `scripts/setup-nifi-flow.py`)
Real NiFi flow with three processors:
- `ConsumeKafka` — reads from `transactions-topic`
- `JoltTransformJSON` — renames `user_id → account_id` and `transaction_date → timestamp`
- `PublishKafka` — publishes to `transactions-processed`

The Java service also runs a `NiFiTransformerService` that reads from `transactions-topic` independently and persists records to `transactions_target`.

**Phase 3 — Python Reconciliation** (`scripts/reconciliation.py`)
Connects to PostgreSQL and validates that `transactions_source` and `transactions_target` are consistent: record counts, NULL checks, and full cross-table reconciliation by transaction ID.

---

## Running from scratch

### Prerequisites

- Docker & Docker Compose
- Python 3 + `psycopg2-binary` (for reconciliation only)
- Ports `8080`, `5432`, `9092`, `2181` free

### Step 1 — Start the infrastructure

```bash
docker-compose up -d postgres kafka zookeeper
```

This starts PostgreSQL (with the schema from `db/init.sql`) and the Kafka broker. Wait about 15 seconds for them to be fully ready.

```bash
# Verify postgres is accepting connections
docker exec gig-postgres pg_isready -U gig_user -d gig_db

# Verify kafka is up (should list __consumer_offsets)
docker exec gig-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Step 2 — Start NiFi and configure the flow

```bash
docker-compose up -d nifi
```

Run the setup script — it waits for NiFi to be ready automatically (up to 2 minutes), then creates the transformation flow:

```bash
pip install requests   # first time only
python3 scripts/setup-nifi-flow.py
```

The script creates three processors via the NiFi REST API:
`ConsumeKafka → JoltTransformJSON → PublishKafka`

Once it completes you can inspect the flow in the NiFi UI at `https://localhost:8161/nifi` (accept the self-signed certificate warning):
- Username: `admin`
- Password: `admin123456789`

### Step 3 — Build and start the Java service

```bash
docker-compose up -d --build java-service
```

The first build takes 2-3 minutes because Maven downloads dependencies. Subsequent builds are faster.

Wait for the service to finish starting:

```bash
docker-compose logs -f java-service
# Wait until you see: "Started GigMigrationApplication"
# Then Ctrl+C to exit the log tail
```

Or just poll the health endpoint:

```bash
curl http://localhost:8080/api/transactions/health
# Expected: {"status":"running"}
```

### Step 4 — Trigger the pipeline

```bash
curl -X POST http://localhost:8080/api/transactions/load-csv
# Expected: {"message":"CSV loading triggered","rows":10}
```

This reads `data/transactions.csv` and publishes each row to Kafka. The transformer picks them up, writes to `transactions_target`, and forwards to `transactions-processed`. The consumer then saves them to `transactions_source`. The whole flow completes in a few seconds.

### Step 5 — Verify via REST API

```bash
# All transactions in the source table
curl http://localhost:8080/api/transactions | jq .

# Single transaction
curl http://localhost:8080/api/transactions/TXN001 | jq .
```

You can also run the full API verification script (requires `jq`):

```bash
./scripts/verify-api.sh
```

### Step 6 — Run Phase 3 reconciliation

```bash
pip install psycopg2-binary   # first time only
python3 scripts/reconciliation.py
```

Expected output:

```
Starting Phase 3 Reconciliation...
Running checks:
  [PASS] Record count — source: 10, target: 10
  [PASS] Data integrity — 0 issue(s)
  [PASS] Reconciliation — 0 mismatches, 0 missing in target, 0 missing in source

Overall result : PASS
```

---

## Reset and retest from scratch

After the first run, use this sequence to wipe everything and go through the pipeline again from zero.

```bash
# 1. Tear down all containers and delete DB volumes
docker-compose down -v

# 2. Start infrastructure and NiFi
docker-compose up -d postgres kafka zookeeper nifi

# 3. Wait ~15s for infra, then build and start the Java service
docker-compose up -d --build java-service

# 4. Wait for NiFi to be ready, then set up the flow
#    (NiFi takes ~60-90s to start)
python3 scripts/setup-nifi-flow.py

# 5. Wait until the Java service is up
until curl -s http://localhost:8080/api/transactions/health | grep -q running; do sleep 3; done

# 6. Trigger the pipeline
curl -X POST http://localhost:8080/api/transactions/load-csv

# 7. Wait a few seconds, then verify
sleep 5
curl http://localhost:8080/api/transactions | jq .

# 8. Run reconciliation
python3 scripts/reconciliation.py
```

---

## Useful commands

```bash
# Follow Java service logs
docker-compose logs -f java-service

# Check record counts directly in Postgres
docker exec gig-postgres psql -U gig_user -d gig_db \
  -c "SELECT COUNT(*) FROM gig.transactions_source;"
docker exec gig-postgres psql -U gig_user -d gig_db \
  -c "SELECT COUNT(*) FROM gig.transactions_target;"

# List Kafka topics
docker exec gig-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## Project structure

```
├── docker-compose.yml
├── data/
│   └── transactions.csv              # 10 sample transactions
├── db/
│   └── init.sql                      # schema: transactions_source + transactions_target
├── java-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/gig/
│       ├── controller/               # REST endpoints
│       ├── kafka/                    # producer, consumer, NiFi transformer
│       ├── model/                    # Transaction, TransactionTarget
│       ├── repository/               # JPA repos
│       └── service/                  # business logic, CSV loading
├── scripts/
│   ├── setup-nifi-flow.py            # configures Phase 2 NiFi flow via REST API
│   ├── reconciliation.py             # Phase 3 validation
│   └── verify-api.sh                 # API smoke tests
└── nifi/
    ├── conf/                         # NiFi configuration (flow, credentials, certs)
    └── flow/migration-flow.xml       # original placeholder (superseded by setup script)
```

## Tech stack

| Component | Technology |
|-----------|-----------|
| REST API + pipeline | Java 17 / Spring Boot 3 |
| Transformation | Apache NiFi 2.9 |
| Messaging | Apache Kafka |
| Database | PostgreSQL 15 |
| Validation | Python 3 |
| Infrastructure | Docker Compose |
