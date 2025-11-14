import csv
import json
import logging
import ssl
from datetime import datetime
from pathlib import Path

import redis
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database import SessionLocal
from app.models.product import Product
from app.services.webhook_service import trigger_webhooks_sync
from app.worker import celery_app

CHUNK_SIZE = 5000
logger = logging.getLogger(__name__)


def _count_total_rows(file_path: str) -> int:
    """Count rows in CSV, skipping header."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        return sum(1 for row in reader)


def _process_chunk(db: Session, chunk: list[dict]):
    """Process chunk using bulk query pattern. Handles duplicates within chunk."""
    if not chunk:
        return 0, 0
    
    # Deduplicate chunk: keep last occurrence of each SKU
    seen_skus = {}
    for row_data in chunk:
        seen_skus[row_data['sku']] = row_data
    deduplicated_chunk = list(seen_skus.values())
    
    products_created = 0
    products_updated = 0
    skus_in_chunk = [row['sku'] for row in deduplicated_chunk]
    
    existing_products = db.query(Product).filter(func.lower(Product.sku).in_(skus_in_chunk)).all()
    existing_products_map = {p.sku.lower(): p for p in existing_products}
    
    for row_data in deduplicated_chunk:
        sku = row_data['sku']
        product = existing_products_map.get(sku)
        
        if product:
            product.name = row_data['name']
            product.description = row_data['description']
            products_updated += 1
        else:
            db.add(Product(sku=sku, name=row_data['name'], description=row_data['description'], active=True))
            products_created += 1
    
    return products_created, products_updated


@celery_app.task(bind=True)
def process_csv_import(self, file_path: str, job_id: str):
    """Process CSV import with chunk-based processing."""
    logger.info(f"Starting CSV import: job_id={job_id}, file_path={file_path}")

    try:
        redis_client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None,
            decode_responses=True
        )
    except Exception as e:
        logger.error(f"Failed to create Redis client: {e}")
        raise

    redis_key = f"job:{job_id}"
    db = SessionLocal()
    file_path_obj = Path(file_path)
    
    if not file_path_obj.exists():
        redis_client.set(redis_key, json.dumps({"status": "failed", "message": f"File not found: {file_path}", "progress": 0}))
        return

    total_created = total_updated = total_skipped = processed_count = 0
    errors = []
    last_progress = 0

    try:
        total_rows = _count_total_rows(str(file_path_obj))
        if total_rows == 0:
            redis_client.set(redis_key, json.dumps({"status": "failed", "message": "CSV file is empty or has no data rows", "progress": 0}))
            return

        redis_client.set(redis_key, json.dumps({"status": "processing", "message": f"Reading {total_rows:,} rows...", "progress": 0}))

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
                        total_skipped += 1
                        errors.append(f"Row {idx}: Missing SKU or Name")
                        continue
                    
                    chunk.append({"sku": sku, "name": name, "description": description})
                    
                    if len(chunk) >= CHUNK_SIZE or idx == total_rows:
                        try:
                            created, updated = _process_chunk(db, chunk)
                            db.commit()
                            total_created += created
                            total_updated += updated
                            processed_count += len(chunk)
                            chunk = []
                            
                            progress_float = (processed_count / total_rows) * 90
                            calculated_progress = min(90, int(round(progress_float)))
                            progress = min(90, last_progress + 1) if calculated_progress <= last_progress else calculated_progress
                            last_progress = progress
                            
                            should_update = (
                                (total_rows >= 100000 and (processed_count % 10000 == 0)) or
                                (total_rows >= 10000 and (processed_count % 1000 == 0)) or
                                (total_rows >= 100 and (processed_count % 100 == 0)) or
                                total_rows < 100 or idx == total_rows
                            )
                            
                            if should_update:
                                redis_client.set(redis_key, json.dumps({
                                    "status": "processing",
                                    "message": f"Processing row {processed_count:,} of {total_rows:,}...",
                                    "progress": progress
                                }))
                        except IntegrityError as e:
                            db.rollback()
                            # Process items individually to handle race conditions
                            chunk_start = idx - len(chunk) + 1
                            individual_created = 0
                            individual_updated = 0
                            individual_skipped = 0
                            
                            for row_idx, row_data in enumerate(chunk):
                                try:
                                    existing = db.query(Product).filter(func.lower(Product.sku) == row_data['sku']).first()
                                    if existing:
                                        existing.name = row_data['name']
                                        existing.description = row_data['description']
                                        db.commit()
                                        individual_updated += 1
                                    else:
                                        db.add(Product(sku=row_data['sku'], name=row_data['name'], description=row_data['description'], active=True))
                                        db.commit()
                                        individual_created += 1
                                except IntegrityError:
                                    db.rollback()
                                    # SKU exists now, update it
                                    existing = db.query(Product).filter(func.lower(Product.sku) == row_data['sku']).first()
                                    if existing:
                                        existing.name = row_data['name']
                                        existing.description = row_data['description']
                                        db.commit()
                                        individual_updated += 1
                                    else:
                                        individual_skipped += 1
                                        errors.append(f"Row {chunk_start + row_idx}: Could not process SKU {row_data['sku']}")
                                except Exception as row_e:
                                    db.rollback()
                                    individual_skipped += 1
                                    errors.append(f"Row {chunk_start + row_idx}: Error - {str(row_e)}")
                            
                            total_created += individual_created
                            total_updated += individual_updated
                            total_skipped += individual_skipped
                            processed_count += len(chunk)
                            chunk = []
                            
                            if individual_skipped > 0:
                                logger.warning(f"IntegrityError on chunk, processed individually: {individual_skipped} skipped")
                        except Exception as e:
                            db.rollback()
                            chunk_start = idx - len(chunk) + 1
                            total_skipped += len(chunk)
                            chunk = []
                            errors.append(f"Rows {chunk_start}-{idx}: Error processing chunk - {str(e)}")
                            logger.error(f"Error processing chunk: {e}")
                except Exception as e:
                    total_skipped += 1
                    errors.append(f"Row {idx}: Error - {str(e)}")
                    logger.warning(f"Error processing row {idx}: {e}")
                    continue
        
        redis_client.set(redis_key, json.dumps({
            "status": "processing",
            "message": f"Finalizing import... ({total_rows:,} rows processed)",
            "progress": 95
        }))
        
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
        
        try:
            trigger_webhooks_sync(db, "import.complete", {
                "event_type": "import.complete",
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "data": final_payload
            })
        except Exception as e:
            logger.warning(f"Failed to trigger 'import.complete' webhook: {e}")
            
    except Exception as e:
        db.rollback()
        logger.error(f"Fatal error processing CSV: {e}", exc_info=True)
        fail_payload = {"status": "failed", "message": f"Fatal error: {str(e)}", "progress": 0}
        redis_client.set(redis_key, json.dumps(fail_payload))
        
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
        if file_path_obj.exists():
            try:
                file_path_obj.unlink()
            except Exception as e:
                logger.error(f"Failed to delete temp file: {e}")
        db.close()
        if redis_client:
            try:
                redis_client.close()
            except Exception:
                pass
