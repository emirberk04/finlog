# Deployment (all free)

| Part | Service |
|---|---|
| Kafka | Aiven for Apache Kafka - Free plan |
| PostgreSQL | Aiven for PostgreSQL - Free plan |
| API + UI | Render - Free web service |

## 1. Aiven (Kafka + PostgreSQL)

1. Sign up at https://console.aiven.io (no credit card needed).
2. Create service -> Aiven for Apache Kafka -> Free plan. Wait until it says *Running*.
3. In the Kafka service open **Topics** and add two topics:
   `transaction-events` and `transaction-logs` (2 partitions is enough).
4. On the **Overview** page copy the *Service URI* (host:port) and download
   `ca.pem`, `service.cert`, `service.key`.
5. Create service -> Aiven for PostgreSQL -> Free plan. Copy its *Service URI*
   (starts with `postgres://`).

Note: free Aiven services are turned off after a long time without use.
Before the presentation open the console and check that both are *Running*.

## 2. Check locally

Put the values in `.env` (see `.env.example`, use `KAFKA_SECURITY_PROTOCOL=SSL`
and paste the text of the 3 files), then run

```bash
python -m scripts.kafka_check
uvicorn app.main:app
```

## 3. Render

1. Push the repo to GitHub.
2. https://dashboard.render.com -> New -> Blueprint -> choose the repo
   (it reads `render.yaml`). Or New -> Web Service with
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
   - Instance type: Free
3. Environment variables:
   `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL=SSL`,
   `KAFKA_CA_CERT`, `KAFKA_CLIENT_CERT`, `KAFKA_CLIENT_KEY` (paste whole file text).
4. After deploy open `https://<name>.onrender.com/health`, it should show
   `"kafka": "ok"` and `"database": "ok"`.

Note: a free Render service sleeps after 15 minutes without requests and the
first request takes about 1 minute. Open the site a few minutes before the demo.
The Kafka consumers also sleep with it; messages wait in Kafka and are processed
when the service wakes up.
