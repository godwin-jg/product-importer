import ssl

from celery import Celery

from app.core.config import settings

celery_app = Celery("product_importer")

celery_app.conf.update(
    broker_url=settings.REDIS_URL,
    result_backend=settings.REDIS_URL,
    result_expires=3600,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=120 * 60,  # 2 hours for large imports
    task_soft_time_limit=110 * 60,  # 110 minutes soft limit
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=10,
    result_backend_transport_options={
        "retry_policy": {
            "timeout": 10.0,
            "max_retries": 5,
            "interval_start": 0.5,
            "interval_step": 0.5,
            "interval_max": 2.0,
        },
        "visibility_timeout": 3600,
        "socket_connect_timeout": 10,
        "socket_timeout": 10,
        "retry_on_timeout": True,
        "health_check_interval": 30,
        "socket_keepalive": True,
        "socket_keepalive_options": {},
    },
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

