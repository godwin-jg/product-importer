import ssl

from celery import Celery

from app.core.config import settings

celery_app = Celery("product_importer")

celery_app.conf.update(
    broker_url=settings.REDIS_URL,
    result_backend=settings.REDIS_URL,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,
    task_soft_time_limit=25 * 60,
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=1000,
)

if "rediss" in settings.REDIS_URL:
    ssl_config = {
        "ssl_cert_reqs": ssl.CERT_NONE,
        "ssl_ca_certs": None,
        "ssl_certfile": None,
        "ssl_keyfile": None,
    }
    celery_app.conf.update(
        broker_use_ssl=ssl_config,
        redis_backend_use_ssl=ssl_config,
    )

celery_app.autodiscover_tasks(["app"])
from app.services import importer  # noqa: F401

