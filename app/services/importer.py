# base64 import removed - we now download files directly from URLs
import csv
import json
import logging
import ssl
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

import httpx
import redis
from celery import group
from redis.exceptions import ResponseError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database import SessionLocal
from app.models.product import Product
from app.services.webhook_service import trigger_webhooks_sync
from app.worker import celery_app

BASE_CHUNK_SIZE = 500
MIN_CHUNK_SIZE = 500
MAX_CHUNK_SIZE = 5000
TARGET_CHUNKS = 100
logger = logging.getLogger(__name__)

_redis_pool = None


def _get_redis_pool():
    """Get or create a shared Redis connection pool."""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = redis.ConnectionPool.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
            socket_keepalive=True,
            socket_keepalive_options={},
            max_connections=50
        )
    return _redis_pool


def _calculate_optimal_chunk_size(total_rows: int) -> int:
    """Calculate optimal chunk size based on total rows."""
    if total_rows == 0:
        return BASE_CHUNK_SIZE
    optimal_size = max(MIN_CHUNK_SIZE, min(total_rows // TARGET_CHUNKS, MAX_CHUNK_SIZE))
    return (optimal_size // 100) * 100  # Round to nearest 100


def _get_redis_client_with_retry(max_retries=3, base_delay=0.5):
    """Get Redis client using shared connection pool with retry logic."""
    pool = _get_redis_pool()
    for attempt in range(max_retries):
        try:
            client = redis.Redis(connection_pool=pool)
            client.ping()
            return client
        except (ResponseError, RedisConnectionError, RedisTimeoutError, TimeoutError) as e:
            error_msg = str(e)
            is_retryable = any(x in error_msg.lower() for x in ["too many requests", "timeout", "max number of clients"])
            if is_retryable and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Redis error, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                continue
            raise
    raise


def _redis_operation_with_retry(operation, max_retries=3, base_delay=0.5):
    """Execute a Redis operation with retry logic."""
    for attempt in range(max_retries):
        try:
            return operation()
        except (ResponseError, RedisConnectionError, RedisTimeoutError, TimeoutError) as e:
            error_msg = str(e)
            is_retryable = any(x in error_msg.lower() for x in ["too many requests", "timeout", "max number of clients"])
            if is_retryable and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Redis error during operation, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
                continue
            raise
    raise


def _count_total_rows(file_path: str) -> int:
    """Count rows in CSV, skipping header."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        return sum(1 for row in reader)


def _split_csv_into_chunks(file_path: str) -> Tuple[List[List[Dict[str, Any]]], int, List[str], int]:
    """
    Split CSV into chunks and return chunk data, total rows, errors, and chunk size used.
    Returns: (chunks, total_rows, errors, chunk_size_used)
    """
    chunks = []
    errors = []
    total_rows = 0
    
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        return chunks, total_rows, errors, BASE_CHUNK_SIZE
    
    # First pass: count total rows to calculate optimal chunk size
    with open(file_path_obj, 'r', encoding='utf-8') as csvfile:
        sample = csvfile.read(1024)
        csvfile.seek(0)
        delimiter = csv.Sniffer().sniff(sample).delimiter
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        total_rows = sum(1 for _ in reader)
    
    # Calculate optimal chunk size based on total rows
    chunk_size = _calculate_optimal_chunk_size(total_rows)
    
    # Second pass: split into chunks using optimal size
    with open(file_path_obj, 'r', encoding='utf-8') as csvfile:
        sample = csvfile.read(1024)
        csvfile.seek(0)
        delimiter = csv.Sniffer().sniff(sample).delimiter
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        chunk = []
        
        for idx, row in enumerate(reader, 1):
            try:
                row_normalized = {k.strip().lower(): v.strip() if v else '' for k, v in row.items()}
                sku = row_normalized.get('sku', '').strip().lower()
                name = row_normalized.get('name', '').strip()
                description = row_normalized.get('description', '').strip() or None
                
                if not sku or not name:
                    errors.append(f"Row {idx}: Missing SKU or Name")
                    continue
                
                chunk.append({"sku": sku, "name": name, "description": description, "row_index": idx})
                
                if len(chunk) >= chunk_size:
                    chunks.append(chunk)
                    chunk = []
            except Exception as e:
                errors.append(f"Row {idx}: Error - {str(e)}")
                logger.warning(f"Error processing row {idx}: {e}")
                continue
        
        # Add remaining chunk
        if chunk:
            chunks.append(chunk)
    
    return chunks, total_rows, errors, chunk_size


def _process_chunk(db: Session, chunk: list[dict], max_retries: int = 3):
    """Process chunk using optimized bulk operations with deadlock retry."""
    if not chunk:
        return 0, 0, 0
    
    # Deduplicate chunk
    seen_skus = {}
    for row_data in chunk:
        seen_skus[row_data['sku']] = row_data
    deduplicated_chunk = list(seen_skus.values())
    
    products_created = 0
    products_updated = 0
    products_skipped = 0
    skus_in_chunk = [row['sku'] for row in deduplicated_chunk]
    
    for attempt in range(max_retries):
        try:
            existing_products = db.query(Product).filter(Product.sku.in_(skus_in_chunk)).all()
            existing_products_map = {p.sku: p for p in existing_products}
            
            to_create = []
            products_created = 0
            products_updated = 0
            
            for row_data in deduplicated_chunk:
                sku = row_data['sku']
                product = existing_products_map.get(sku)
                
                if product:
                    product.name = row_data['name']
                    product.description = row_data['description']
                    products_updated += 1
                else:
                    to_create.append({
                        'sku': sku,
                        'name': row_data['name'],
                        'description': row_data['description'],
                        'active': True
                    })
                    products_created += 1
            
            if to_create:
                BATCH_INSERT_SIZE = 200
                for i in range(0, len(to_create), BATCH_INSERT_SIZE):
                    db.bulk_insert_mappings(Product, to_create[i:i + BATCH_INSERT_SIZE])
            
            db.commit()
            return products_created, products_updated, products_skipped

        except IntegrityError as e:
            db.rollback()
            logger.warning(f"Chunk IntegrityError (race condition), skipping. {e}")
            products_skipped = len(deduplicated_chunk)
            products_created = 0
            products_updated = 0
            return products_created, products_updated, products_skipped
            
        except OperationalError as e:
            error_str = str(e.orig) if hasattr(e, 'orig') else str(e)
            is_deadlock = "deadlock detected" in error_str.lower() or "40001" in error_str
            is_timeout = "statement timeout" in error_str.lower() or "querycanceled" in error_str.lower() or "57014" in error_str
            
            if (is_deadlock or is_timeout) and attempt < max_retries - 1:
                db.rollback()
                wait_time = 0.1 * (2 ** attempt)
                error_type = "deadlock" if is_deadlock else "statement timeout"
                logger.warning(f"Chunk {error_type} detected (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
                continue
            else:
                db.rollback()
                if is_deadlock:
                    logger.error(f"Chunk deadlock after {max_retries} retries, skipping. {e}")
                elif is_timeout:
                    logger.error(f"Chunk statement timeout after {max_retries} retries, skipping. {e}")
                else:
                    logger.error(f"Chunk OperationalError: {e}", exc_info=True)
                products_skipped = len(deduplicated_chunk)
                products_created = 0
                products_updated = 0
                return products_created, products_updated, products_skipped
                
        except Exception as e:
            db.rollback()
            logger.error(f"Error processing chunk: {e}", exc_info=True)
            products_skipped = len(deduplicated_chunk)
            products_created = 0
            products_updated = 0
            return products_created, products_updated, products_skipped
    
    return 0, 0, len(deduplicated_chunk)


@celery_app.task(
    bind=True,
    ignore_result=True,
    autoretry_for=(
        ResponseError,
        RedisConnectionError,
        RedisTimeoutError,
        TimeoutError,
        IntegrityError,
        OperationalError,
    ),
    retry_kwargs={'max_retries': 3, 'countdown': 2},
    retry_backoff=True,
    retry_backoff_max=30,
    retry_jitter=True,
)
def process_csv_chunk(self, chunk_data: List[Dict[str, Any]], job_id: str, chunk_index: int, total_chunks: int):
    """
    Process a single chunk of CSV data in parallel.
    This task is called by process_csv_import for each chunk.
    """
    db = None
    redis_client = None
    
    # Define Redis keys once
    chunks_hash_key = f"job:{job_id}:chunks"
    counter_key = f"job:{job_id}:completed_count"
    
    try:
        redis_client = _get_redis_client_with_retry()
        
        db = SessionLocal()
        
        logger.info(f"Processing chunk {chunk_index + 1}/{total_chunks} for job {job_id} (chunk size: {len(chunk_data)} rows)")
        
        # Process the chunk
        chunk_start_time = time.time()
        created, updated, skipped = _process_chunk(db, chunk_data)
        chunk_duration = time.time() - chunk_start_time
        logger.info(f"Chunk {chunk_index + 1}/{total_chunks} database processing took {chunk_duration:.2f}s")
        
        # Return result
        result = {
            "chunk_index": chunk_index,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "processed": len(chunk_data),
            "status": "success"
        }
        
        # Update chunk status AND increment counter atomically using pipeline
        # Use retry logic to handle rate limiting
        _redis_operation_with_retry(
            lambda: redis_client.pipeline()
                .hset(chunks_hash_key, str(chunk_index), json.dumps(result))
                .incr(counter_key)
                .execute()
        )
        
        logger.info(f"Chunk {chunk_index + 1}/{total_chunks} completed: {created} created, {updated} updated, {skipped} skipped")
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing chunk {chunk_index + 1} for job {job_id}: {e}", exc_info=True)
        
        # Mark chunk as failed
        result = {
            "chunk_index": chunk_index,
            "created": 0,
            "updated": 0,
            "skipped": len(chunk_data),
            "processed": 0,
            "status": "failed",
            "error": str(e)
        }
        
        if redis_client:
            # Update chunk status AND increment counter on failure too
            # Use retry logic to handle rate limiting
            try:
                _redis_operation_with_retry(
                    lambda: redis_client.pipeline()
                        .hset(chunks_hash_key, str(chunk_index), json.dumps(result))
                        .incr(counter_key)
                        .execute()
                )
            except Exception as redis_err:
                logger.error(f"Failed to update Redis after chunk failure: {redis_err}")
        
        return result
        
    finally:
        if db:
            db.close()
        # Don't close redis_client - it uses a shared connection pool
        # The pool manages connections automatically


@celery_app.task(
    bind=True,
    autoretry_for=(
        ResponseError,
        RedisConnectionError,
        RedisTimeoutError,
        TimeoutError,
        IntegrityError,
        OperationalError,
    ),
    retry_kwargs={'max_retries': 3, 'countdown': 5},
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
)
def run_parallel_import_task(self, local_file_path: str, job_id: str):
    """
    (Task 2) Orchestrates the parallel processing of a downloaded local file.
    This task splits the CSV, dispatches chunks, and monitors progress.
    
    Args:
        local_file_path: Path to the local CSV file
        job_id: Unique job identifier
    """
    redis_client = None
    db = None
    redis_key = f"job:{job_id}"

    try:
        redis_client = _get_redis_client_with_retry()
        
        # Step 1: Split CSV into chunks
        logger.info(f"Splitting CSV into chunks for job {job_id}")
        redis_client.set(redis_key, json.dumps({
            "status": "processing",
            "message": "Reading and splitting CSV file...",
            "progress": 0
        }))
        
        chunks, total_rows, initial_errors, chunk_size_used = _split_csv_into_chunks(local_file_path)
        
        if total_rows == 0:
            redis_client.set(redis_key, json.dumps({
                "status": "failed",
                "message": "CSV file is empty or has no data rows",
                "progress": 0
            }))
            return

        total_chunks = len(chunks)
        logger.info(f"Split CSV into {total_chunks} chunks for {total_rows:,} rows (job {job_id})")
        
        if total_chunks == 0:
            redis_client.set(redis_key, json.dumps({
                "status": "failed",
                "message": "No valid data rows found in CSV",
                "progress": 0
            }))
            return

        # Initialize chunk tracking in Redis
        chunks_key = f"job:{job_id}:chunks"
        redis_client.delete(chunks_key)  # Clear any existing data
        redis_client.set(f"job:{job_id}:total_chunks", total_chunks)
        redis_client.set(f"job:{job_id}:total_rows", total_rows)
        
        # Step 2: Dispatch all chunk tasks in parallel using Celery group
        logger.info(f"Dispatching {total_chunks} chunk tasks in parallel for job {job_id}")
        redis_client.set(redis_key, json.dumps({
            "status": "processing",
            "message": f"Dispatching {total_chunks:,} chunks to workers...",
            "progress": 5
        }))
        
        # Create a group of tasks - all will execute in parallel
        chunk_tasks = [
            process_csv_chunk.s(chunk, job_id, idx, total_chunks)
            for idx, chunk in enumerate(chunks)
        ]
        
        job = group(chunk_tasks)
        result_group = job.apply_async()
        
        # Update status to show chunks are being processed
        try:
            redis_client.set(redis_key, json.dumps({
                "status": "processing",
                "message": f"Processing {total_chunks:,} chunks in parallel ({total_rows:,} rows)...",
                "progress": 5
            }))
        except Exception as e:
            logger.error(f"Failed to update Redis after dispatching: {e}")
        
        # Step 3: Monitor progress and collect results using fast atomic counter
        logger.info(f"Monitoring {total_chunks} parallel chunk tasks for job {job_id}")
        
        # Define keys for monitoring
        counter_key = f"job:{job_id}:completed_count"
        redis_client.set(counter_key, 0)  # Initialize counter
        
        completed_count = 0
        
        # Monitor progress using atomic counter
        while completed_count < total_chunks:
            try:
                completed_count = int(_redis_operation_with_retry(
                    lambda: redis_client.get(counter_key) or 0
                ))
            except Exception:
                pass
            
            progress = min(90, int(5 + (completed_count / total_chunks) * 85))
            message = (f"Waiting for workers... ({total_chunks:,} chunks queued)" if completed_count == 0
                      else f"Processing chunks: {completed_count:,}/{total_chunks:,} complete...")
            
            try:
                _redis_operation_with_retry(
                    lambda: redis_client.set(redis_key, json.dumps({
                        "status": "processing",
                        "message": message,
                        "progress": progress
                    }))
                )
            except Exception:
                pass
            
            if result_group.ready():
                time.sleep(0.5)
                try:
                    completed_count = int(_redis_operation_with_retry(
                        lambda: redis_client.get(counter_key) or 0
                    ))
                except Exception:
                    pass
                if completed_count >= total_chunks:
                    break
            
            time.sleep(0.5)
        
        # Step 4: Finalize and aggregate results
        # NOW it is safe to run the slow HGETALL command *ONCE*.
        logger.info(f"All {total_chunks} chunks completed. Aggregating results...")
        redis_client.set(redis_key, json.dumps({
            "status": "processing",
            "message": f"Finalizing import...",
            "progress": 95
        }))
        
        total_created = 0
        total_updated = 0
        total_skipped = 0
        processed_count = 0
        errors = initial_errors.copy()
        
        # Now do the expensive hgetall operation ONCE to aggregate results
        chunk_results = redis_client.hgetall(chunks_key)
        for chunk_result_json in chunk_results.values():
            try:
                chunk_result = json.loads(chunk_result_json)
                total_created += chunk_result.get("created", 0)
                total_updated += chunk_result.get("updated", 0)
                total_skipped += chunk_result.get("skipped", 0)
                processed_count += chunk_result.get("processed", 0)
                if chunk_result.get("status") == "failed":
                    chunk_idx = chunk_result.get("chunk_index", "N/A")
                    error_msg = chunk_result.get("error", "Unknown")
                    errors.append(f"Chunk {chunk_idx + 1}: {error_msg}")
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Error parsing final chunk result: {e}")
        
        # Clean up ALL monitoring keys
        redis_client.delete(chunks_key)
        redis_client.delete(counter_key)
        redis_client.delete(f"job:{job_id}:total_chunks")
        redis_client.delete(f"job:{job_id}:total_rows")
        
        message_parts = []
        if total_created > 0:
            message_parts.append(f"{total_created:,} created")
        if total_updated > 0:
            message_parts.append(f"{total_updated:,} updated")
        if total_skipped > 0:
            message_parts.append(f"{total_skipped:,} skipped")
        
        message = f"Import complete: {', '.join(message_parts) or 'No changes'}"
        if errors:
            message += f" ({len(errors)} errors)"

        final_payload = {
            "status": "complete",
            "message": message,
            "progress": 100,
            "created": total_created,
            "updated": total_updated,
            "skipped": total_skipped,
            "total_rows": total_rows
        }
        redis_client.set(redis_key, json.dumps(final_payload))
        
        # Trigger webhook
        db = SessionLocal()
        try:
            trigger_webhooks_sync(db, "import.complete", {
                "event_type": "import.complete",
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": final_payload
            })
        except Exception as e:
            logger.warning(f"Failed to trigger 'import.complete' webhook: {e}")
        finally:
            if db:
                db.close()
            
    except Exception as e:
        logger.error(f"Fatal error processing CSV: {e}", exc_info=True)
        fail_payload = {
            "status": "failed",
            "message": f"Fatal error: {str(e)}",
            "progress": 0
        }
        if redis_client:
            redis_client.set(redis_key, json.dumps(fail_payload))
        
        # Clean up chunk tracking
        try:
            if redis_client:
                redis_client.delete(f"job:{job_id}:chunks")
                redis_client.delete(f"job:{job_id}:completed_count")
                redis_client.delete(f"job:{job_id}:total_chunks")
                redis_client.delete(f"job:{job_id}:total_rows")
        except Exception:
            pass
        
        # Trigger failure webhook
        if db is None:
            db = SessionLocal()
        try:
            trigger_webhooks_sync(db, "import.failed", {
                "event_type": "import.failed",
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": fail_payload
            })
        except Exception as e_wh:
            logger.warning(f"Failed to trigger 'import.failed' webhook: {e_wh}")
        finally:
            if db:
                db.close()
    
    finally:
        # --- MOVED FROM process_csv_import ---
        # This task is the *last* one to touch the file,
        # so it is responsible for cleaning it up.
        if local_file_path:
            try:
                Path(local_file_path).unlink(missing_ok=True)
                logger.info(f"Cleaned up temporary file: {local_file_path}")
            except Exception as e:
                logger.error(f"Failed to delete temp file: {e}")


@celery_app.task(
    bind=True,
    autoretry_for=(
        ResponseError,
        RedisConnectionError,
        RedisTimeoutError,
        TimeoutError,
    ),  # Note: Removed DB errors, as this task doesn't touch the DB
    retry_kwargs={'max_retries': 3, 'countdown': 5},
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
)
def process_csv_import(self, file_url: str, job_id: str):
    """
    (Task 1) Download CSV file from Vercel Blob and trigger the orchestrator.
    This task only downloads the file and chains to the orchestrator task.
    
    Args:
        file_url: Vercel Blob URL of the uploaded CSV file
        job_id: Unique job identifier
    """
    logger.info(f"Starting import from URL: {file_url}, job_id={job_id}")
    
    redis_client = None
    redis_key = f"job:{job_id}"
    local_file_path = None

    try:
        redis_client = _get_redis_client_with_retry()
        
        # Immediately update status to show task has started
        _redis_operation_with_retry(
            lambda: redis_client.set(redis_key, json.dumps({
                "status": "processing",
                "message": "Task started, downloading file...",
                "progress": 0
            }))
        )
    except Exception as e:
        logger.error(f"Failed to create Redis client: {e}")
        raise  # Let Celery retry
    
    # Download file from Cloudinary
    try:
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            local_file_path = temp_file.name
            
            logger.info(f"Downloading file from Vercel Blob to {local_file_path}")
            with httpx.stream("GET", file_url, timeout=300.0) as response:
                response.raise_for_status()  # Check for download errors
                for chunk in response.iter_bytes():
                    temp_file.write(chunk)
            
            logger.info("Download complete.")
            
        # --- SUCCESS: CHAIN TO THE NEXT TASK ---
        # Instead of calling the function, we dispatch the new task
        run_parallel_import_task.delay(local_file_path, job_id)
        # This task is now FINISHED and the worker is freed.
            
    except Exception as e:
        # --- DOWNLOAD FAILED ---
        error_msg = f"Failed to download file from Vercel Blob: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if redis_client:
            redis_client.set(redis_key, json.dumps({
                "status": "failed",
                "message": error_msg,
                "progress": 0
            }))
            
        # If download fails, we *must* clean up the temp file
        if local_file_path:
            try:
                Path(local_file_path).unlink(missing_ok=True)
                logger.info(f"Cleaned up failed download: {local_file_path}")
            except Exception as e_clean:
                logger.error(f"Failed to delete temp file after error: {e_clean}")
        
        # Re-raise the exception to trigger Celery retry
        raise
    
    # --- REMOVED THE 'finally' BLOCK ---
    # The new `run_parallel_import_task` is now responsible for cleanup.
