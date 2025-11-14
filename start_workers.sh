#!/bin/bash
# Script to start multiple Celery workers for parallel processing

# Number of worker processes (adjust based on your CPU cores)
WORKERS=${1:-4}

echo "Starting Celery workers with concurrency=$WORKERS..."

# Start worker with specified concurrency
celery -A app.worker.celery_app worker \
    --loglevel=info \
    --concurrency=$WORKERS \
    --pool=prefork

