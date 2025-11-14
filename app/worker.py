from celery import Celery

from app.core.config import settings

# Create Celery app instance
celery_app = Celery(
    "product_importer",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

# Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
)

# Auto-discover tasks from the 'tasks' module in the app directory
celery_app.autodiscover_tasks(["app"])

