import logging
import ssl
from contextlib import asynccontextmanager
from pathlib import Path

import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.api import product_routes, upload_routes, webhook_routes
from app.core.config import settings
from app.database import Base, engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Static files directory - use absolute path from project root
BASE_DIR = Path(__file__).resolve().parent.parent
static_dir = BASE_DIR / "static"


def mask_url_password(url: str) -> str:
    """Mask password in URL for logging."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.password:
            return url.replace(f":{parsed.password}@", ":****@")
        return url
    except Exception:
        return url


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected to database: {mask_url_password(settings.DATABASE_URL)}")
        Base.metadata.create_all(bind=engine)
    except OperationalError as e:
        logger.warning(f"Database connection failed: {e}")
    
    try:
        redis_client = redis.from_url(settings.REDIS_URL, ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None)
        redis_client.ping()
        logger.info(f"Connected to Redis: {mask_url_password(settings.REDIS_URL)}")
        redis_client.close()
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
    
    yield


app = FastAPI(lifespan=lifespan)

# Mount static files first (before routes)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Include routers
app.include_router(product_routes.router)
app.include_router(upload_routes.router)
app.include_router(webhook_routes.router)


@app.get("/")
async def root():
    """Serve the frontend index page."""
    return FileResponse(
        static_dir / "index.html",
        media_type="text/html"
    )

