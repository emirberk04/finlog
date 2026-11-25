# FinLog - Financial Transaction Logging Service

SENG315 / SENG451 Homework II - Group 7

A small system where users record money transactions (deposit, withdrawal, payment)
and see filtered lists and a summary report. Every new transaction is sent to Kafka
and processed in the background; the result is logged through a second Kafka topic.

| Layer | Technology | Folder | Owner |
|---|---|---|---|
| API | FastAPI (REST) | `app/api/`, `app/main.py`, `ui/` | Emir Berk Alan |
| Messaging | Apache Kafka (aiokafka) | `app/messaging/` | Nourıa Mahamoud Osman |
| Data | SQLAlchemy ORM + PostgreSQL (SQLite locally) | `app/data/` | Ela Soydan |

## How it works

```
UI / Postman --HTTP--> FastAPI --(save PENDING)--> DB
                          |
                          +--> Kafka topic "transaction-events"
                                   |
                          processor consumer: check balance, update DB (COMPLETED / REJECTED)
                                   |
                                   +--> Kafka topic "transaction-logs"
                                            |
                                   log-writer consumer: save log row to DB
```

## Run on your computer

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d          # local Kafka on localhost:9092
cp .env.example .env
uvicorn app.main:app --reload
```

Open http://localhost:8000 for the UI and http://localhost:8000/docs for the API docs.
Without `DATABASE_URL` a SQLite file (`finlog.db`) is used.

Test the Kafka settings: `python -m scripts.kafka_check`

## Tests

```bash
pytest -v
```

The tests use SQLite and a fake broker (`tests/conftest.py`), so they run without Kafka.

## Endpoints

| Method | Path | What it does |
|---|---|---|
| POST | /api/accounts | create account |
| GET | /api/accounts, /api/accounts/{id} | list / get accounts |
| POST | /api/transactions | new transaction (202, processed by Kafka consumer) |
| GET | /api/transactions | list with filters: account_id, type, status, category, min_amount, max_amount, date_from, date_to, limit, offset |
| GET | /api/transactions/{id} | one transaction |
| GET | /api/transactions/{id}/logs | log history of a transaction |
| GET | /api/reports/summary | totals, counts by status, spending by category |
| GET | /api/messaging/activity | last Kafka messages (for the demo) |
| GET | /health | API, database and Kafka status |

Postman collection: `docs/FinLog.postman_collection.json`

## Deployment

See `docs/DEPLOY.md` (Aiven free Kafka + Aiven free PostgreSQL + Render free web service).
