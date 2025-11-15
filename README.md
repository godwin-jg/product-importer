# Product Importer

A scalable product import system that allows you to upload CSV files and import products into a PostgreSQL database. The application processes imports asynchronously using Celery workers, provides real-time job status updates via Redis, and includes a web interface for managing products and monitoring import jobs.

## Tech Stack

- **Backend Framework**: FastAPI
- **Web Server**: Gunicorn with Uvicorn workers
- **Task Queue**: Celery
- **Database**: PostgreSQL
- **Cache/Message Broker**: Redis
- **ORM**: SQLAlchemy
- **Language**: Python 3.14+

## Local Setup

### Prerequisites

- Python 3.14 or higher
- PostgreSQL database
- Redis server

### Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/product_importer
REDIS_URL=redis://localhost:6379/0
```

For Redis with SSL (e.g., Redis Cloud):
```env
REDIS_URL=rediss://user:password@host:port/0
```

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd product_importer
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Application

#### Start the Web Server

Run the FastAPI application using Uvicorn:

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

For production-like setup with Gunicorn:

```bash
gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app
```

#### Start the Celery Worker

In a separate terminal, start the Celery worker to process background tasks:

```bash
celery -A app.worker.celery_app worker --loglevel=info
```

For multiple workers with concurrency:

```bash
bash start_workers.sh 4  # Starts worker with 4 concurrent processes
```

Or manually:

```bash
celery -A app.worker.celery_app worker --loglevel=info --concurrency=4 --pool=prefork
```

### Database Setup

The application will automatically create the necessary database tables on first startup. If you need to run migrations manually, refer to the `migrations/` directory.

## Deployment

This project is configured for deployment on Render using the `render.yaml` Blueprint file.

### Render Configuration

The `render.yaml` file defines the following services:

1. **Web Service** (`product-importer-api`): 
   - Runs the FastAPI application using Gunicorn with 4 Uvicorn workers
   - Automatically receives `DATABASE_URL` and `REDIS_URL` environment variables

2. **Worker Service** (`product-importer-worker`):
   - Runs Celery workers to process background import tasks
   - Shares the same environment variables as the web service

3. **PostgreSQL Database** (`product-importer-db`):
   - Starter plan PostgreSQL database
   - Connection string automatically provided via `DATABASE_URL`

4. **Redis Service** (`product-importer-redis`):
   - Starter plan Redis instance for task queue and job status
   - Connection string automatically provided via `REDIS_URL`

### Deploying to Render

1. Push your code to a Git repository (GitHub, GitLab, or Bitbucket)
2. Connect your repository to Render
3. Render will automatically detect the `render.yaml` file and create all services
4. The environment variables (`DATABASE_URL` and `REDIS_URL`) will be automatically linked from the database and Redis addons to both the web and worker services

### Live Application

[LINK TO DEPLOYED APP]
