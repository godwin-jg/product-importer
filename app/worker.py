import ssl
from urllib.parse import urlparse

from celery import Celery

from app.core.config import settings

# Parse Redis URL to check if SSL is required
redis_url_parsed = urlparse(settings.REDIS_URL)
is_ssl = redis_url_parsed.scheme == "rediss"

# Configure broker and backend URLs
broker_url = settings.REDIS_URL
backend_url = settings.REDIS_URL

# Create Celery app instance
celery_app = Celery("product_importer")

# Configure Celery
celery_app.conf.update(
    broker_url=broker_url,
    result_backend=backend_url,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
)

# Configure SSL for Redis if using rediss://
if is_ssl:
    celery_app.conf.update(
        broker_use_ssl={
            "ssl_cert_reqs": ssl.CERT_NONE,
            "ssl_ca_certs": None,
            "ssl_certfile": None,
            "ssl_keyfile": None,
        },
        redis_backend_use_ssl={
            "ssl_cert_reqs": ssl.CERT_NONE,
            "ssl_ca_certs": None,
            "ssl_certfile": None,
            "ssl_keyfile": None,
        },
    )

# Auto-discover tasks from the 'tasks' module in the app directory
celery_app.autodiscover_tasks(["app"])

