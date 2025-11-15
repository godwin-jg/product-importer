from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

# Configure connection pool for parallel processing
# Increased pool size to support multiple workers processing chunks concurrently
# pool_size: base connections always available
# max_overflow: additional connections that can be created on demand
# Total max connections = pool_size + max_overflow = 50
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=20,  # Increased from 10 to support parallel chunk processing
    max_overflow=30,  # Increased from 20 to allow more concurrent workers
    pool_pre_ping=True,  # Verify connections before using
    pool_recycle=3600,  # Recycle connections after 1 hour
    connect_args={
        "connect_timeout": 10,  # 10 second connection timeout
        "options": "-c statement_timeout=30000"  # 30 second statement timeout
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

