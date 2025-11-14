import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.api import product_routes, upload_routes
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
    """Mask password in URL for logging purposes."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            masked = url.replace(f":{parsed.password}@", ":****@")
            return masked
        return url
    except Exception:
        return url


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    # Test and log database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Successfully connected to database: {mask_url_password(settings.DATABASE_URL)}")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except OperationalError as e:
        logger.warning(f"Could not connect to database on startup: {e}")
        logger.info("Application will continue, but database operations may fail")
    
    # Test and log Redis connection
    try:
        redis_client = redis.from_url(settings.REDIS_URL)
        redis_client.ping()
        logger.info(f"Successfully connected to Redis: {mask_url_password(settings.REDIS_URL)}")
    except Exception as e:
        logger.warning(f"Could not connect to Redis on startup: {e}")
        logger.info("Application will continue, but Redis operations may fail")
    
    yield
    
    # Shutdown (if needed in the future)


app = FastAPI(lifespan=lifespan)

# Mount static files first (before routes)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Include routers
app.include_router(product_routes.router)
app.include_router(upload_routes.router)


@app.get("/")
async def root():
    """Serve the frontend index page."""
    return FileResponse(
        static_dir / "index.html",
        media_type="text/html"
    )

