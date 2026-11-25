# FinLog - Financial Transaction Logging Service
# SENG315 Homework II - Group 7
import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.api.routes import router
from app.config import settings
from app.data.database import SessionLocal, init_db
from app.messaging import consumer, kafka_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("finlog")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    tasks = []
    if settings.kafka_enabled:
        try:
            await kafka_client.start_producer()
        except Exception as e:
            # the API still starts, POST /transactions will answer 503
            log.error("could not connect to Kafka: %s", e)
        tasks = [asyncio.create_task(consumer.run_processor()),
                 asyncio.create_task(consumer.run_log_writer())]
    yield
    for t in tasks:
        t.cancel()
    await kafka_client.stop_producer()


app = FastAPI(title="FinLog - Financial Transaction Logging Service", version="1.0",
              lifespan=lifespan)

# the UI can also be hosted on another domain (Netlify etc.)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(router)


# ---------- error handling: always answer with the same JSON shape ----------

@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    details = []
    for e in exc.errors():
        field = ".".join(str(p) for p in e["loc"] if p not in ("body", "query", "path"))
        details.append({"field": field, "problem": e["msg"].replace("Value error, ", "")})
    return JSONResponse(status_code=422, content={"error": "invalid input", "details": details})


@app.exception_handler(Exception)
async def unexpected_error(request: Request, exc: Exception):
    log.exception("unexpected error")
    return JSONResponse(status_code=500, content={"error": "internal server error"})


@app.get("/health")
def health():
    db_ok = True
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception:
        db_ok = False
    return {"api": "ok", "database": "ok" if db_ok else "down",
            "kafka": "ok" if kafka_client.is_connected() else "down"}


# the simple web UI is served from the same app
UI_DIR = Path(__file__).resolve().parent.parent / "ui"
app.mount("/", StaticFiles(directory=UI_DIR, html=True), name="ui")
