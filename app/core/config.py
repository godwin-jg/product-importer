from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    DATABASE_URL: str
    REDIS_URL: str
    
    # Vercel Blob settings (required for large file uploads)
    BLOB_READ_WRITE_TOKEN: str | None = None
    
    # Celery worker settings (optional)
    CELERY_CONCURRENCY: str | None = None
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # Ignore extra environment variables
    )


settings = Settings()

