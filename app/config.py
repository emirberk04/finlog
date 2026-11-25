# All settings come from environment variables (or a .env file).
# For local work the defaults are enough: SQLite file + Kafka on localhost.
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- data layer ---
    database_url: str = "sqlite:///./finlog.db"

    # --- messaging layer ---
    kafka_bootstrap_servers: str = "localhost:9092"
    # PLAINTEXT for local docker, SSL (client certificate) or SASL_SSL for Aiven
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "SCRAM-SHA-256"
    kafka_username: str = ""
    kafka_password: str = ""
    # contents of ca.pem / service.cert / service.key (not file paths),
    # because on Render it is easier to paste them as env variables
    kafka_ca_cert: str = ""
    kafka_client_cert: str = ""
    kafka_client_key: str = ""

    topic_transactions: str = "transaction-events"
    topic_logs: str = "transaction-logs"
    consumer_group: str = "finlog-processor"
    log_consumer_group: str = "finlog-log-writer"

    # set to false in tests so the app starts without a broker
    kafka_enabled: bool = True


settings = Settings()


def fix_db_url(url: str) -> str:
    # Aiven / Render give "postgres://..." but SQLAlchemy wants the driver name
    if url.startswith("postgres://"):
        url = "postgresql+psycopg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]
    return url
