# Data layer - database connection (SQLAlchemy engine + session)
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings, fix_db_url

DATABASE_URL = fix_db_url(settings.database_url)

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    # SQLite is used only for local runs and tests.
    # Needed because FastAPI and the Kafka consumer use different threads.
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def init_db():
    # create the tables if they are not there yet
    from app.data import models  # noqa: F401  (import so the models are registered)
    Base.metadata.create_all(bind=engine)


def get_db():
    # FastAPI dependency: one session per request, always closed at the end
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
