# Gig Migration Challenge

A three-phase data migration pipeline that ingests transaction data, processes it through Apache NiFi, validates results, and ensures data integrity.

## 🎯 Project Overview

This project demonstrates a complete data migration workflow using modern cloud-native technologies. Transaction data flows through multiple processing stages with validation at each phase.

### Three-Phase Architecture

```
CSV Data → Phase 1 (Java/Kafka) → Phase 2 (NiFi) → Phase 3 (Reconciliation)
```

## 📋 Phases

### Phase 1: Java Service (Ingestion & Initial Processing)
- **Framework**: Spring Boot
- **Responsibilities**:
  - REST API endpoints for transaction processing
  - Kafka producer/consumer for event streaming
  - Database integration with PostgreSQL
  - Initial data validation and transformation
- **Location**: `java-service/`

### Phase 2: Apache NiFi (Orchestration)
- **Tool**: Apache NiFi
- **Responsibilities**:
  - Data flow orchestration and routing
  - Complex transformation logic
  - Multi-stage processing pipeline
  - Data quality checks
- **Location**: `nifi/flow/migration-flow.xml`

### Phase 3: Python Reconciliation
- **Language**: Python
- **Responsibilities**:
  - Post-migration data validation
  - Reconciliation between source and target
  - Data completeness and accuracy checks
- **Location**: `scripts/reconciliation.py`

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ available RAM
- Ports 8080, 5432, 9092 available

### Running the Project

```bash
# Start all services with one command
docker-compose up

# The services will be available at:
# - Java Service: http://localhost:8080
# - PostgreSQL: localhost:5432
# - Kafka: localhost:9092
# - NiFi: http://localhost:8161
```

### Testing the APIs

```bash
# Run the verification script
./scripts/verify-api.sh
```

## 📁 Project Structure

```
gig-migration-challenge/
├── docker-compose.yml          # Orchestration config
├── README.md                   # This file
├── java-service/               # Spring Boot application
│   ├── Dockerfile
│   ├── pom.xml                 # Maven dependencies
│   └── src/
├── nifi/                       # NiFi orchestration
│   └── flow/
│       └── migration-flow.xml
├── data/                       # Source data
│   └── transactions.csv
├── db/                         # Database setup
│   └── init.sql                # PostgreSQL schema
├── scripts/                    # Utilities
│   ├── verify-api.sh
│   └── reconciliation.py
└── docs/
    └── architecture.png        # System diagram
```

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Ingestion | Java/Spring Boot | REST APIs & Kafka integration |
| Messaging | Apache Kafka | Event streaming & decoupling |
| Orchestration | Apache NiFi | Data flow management |
| Database | PostgreSQL | Persistent data storage |
| Validation | Python | Reconciliation & QA |
| Infrastructure | Docker | Containerization & deployment |

## 📊 Data Flow

1. **Source**: Transaction CSV file loaded into PostgreSQL
2. **Phase 1**: Java service reads data, publishes to Kafka topics
3. **Phase 2**: NiFi consumes Kafka events, applies transformations
4. **Target**: Processed data written to final tables
5. **Phase 3**: Python script validates data integrity

## 🧪 Testing

### Verify API Endpoints
```bash
./scripts/verify-api.sh
```

### Run Reconciliation
```bash
python scripts/reconciliation.py
```

## 📝 Configuration

### Application Properties
- Java service config: `java-service/src/main/resources/application.yml`
- Database init: `db/init.sql`
- Docker services: `docker-compose.yml`

## 🔍 Monitoring

- **Java Service Logs**: `docker-compose logs java-service`
- **NiFi UI**: Open http://localhost:8161
- **PostgreSQL**: Connect via any SQL client at localhost:5432

## 📖 Documentation

- Architecture diagram: `docs/architecture.png`
- API verification: `scripts/verify-api.sh` (includes curl examples)
- Reconciliation logic: `scripts/reconciliation.py` (inline comments)

## ✅ Success Criteria

- [ ] Docker Compose starts all services successfully
- [ ] Java service API responds to requests
- [ ] NiFi flow processes data without errors
- [ ] PostgreSQL contains transformed data
- [ ] Reconciliation script validates 100% data integrity
- [ ] All test scripts pass

## 🤝 Contributing

Please ensure:
1. Docker Compose can start cleanly
2. All scripts are executable
3. Documentation is up to date
4. Data validation passes

## 📄 License

This project is part of the Gig Migration Challenge.
