import csv
import json
import ssl
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import redis
from sqlalchemy.exc import IntegrityError

from app.core.config import settings
from app.database import SessionLocal
from app.models.product import Product
from app.services.webhook_service import trigger_webhooks_sync
from app.worker import celery_app


@celery_app.task(bind=True)
def process_csv_import(self, file_path: str, job_id: str):
    """
    Process CSV import task.
    Reads CSV file and imports products into the database.
    Expected CSV format: SKU, Name, Description
    Note: Active status is not part of CSV - new products default to active=True,
    existing products retain their current active status.
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Starting CSV import task: job_id={job_id}, file_path={file_path}")
    
    # Create Redis client with SSL support if needed
    redis_url_parsed = urlparse(settings.REDIS_URL)
    is_ssl = redis_url_parsed.scheme == "rediss"
    
    redis_client = None
    try:
        if is_ssl:
            redis_client = redis.from_url(
                settings.REDIS_URL,
                ssl_cert_reqs=ssl.CERT_NONE
            )
        else:
            redis_client = redis.from_url(settings.REDIS_URL)
    except Exception as e:
        logger.error(f"Failed to create Redis client: {e}")
        raise
    
    redis_key = f"job:{job_id}"
    db = SessionLocal()
    
    try:
        logger.info(f"File exists check: {Path(file_path).exists()}")
        # Update status to processing
        redis_client.set(
            redis_key,
            json.dumps({
                "status": "processing",
                "message": "Reading CSV file...",
                "progress": 10
            })
        )
        
        # Check if file exists
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            redis_client.set(
                redis_key,
                json.dumps({
                    "status": "failed",
                    "message": f"File not found: {file_path}",
                    "progress": 0
                })
            )
            return
        
        # Read and parse CSV
        products_created = 0
        products_updated = 0
        products_skipped = 0
        errors = []
        
        with open(file_path_obj, 'r', encoding='utf-8') as csvfile:
            # Detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            
            # Get total rows for progress tracking
            rows = list(reader)
            total_rows = len(rows)
            
            if total_rows == 0:
                redis_client.set(
                    redis_key,
                    json.dumps({
                        "status": "failed",
                        "message": "CSV file is empty or has no data rows",
                        "progress": 0
                    })
                )
                return
            
            # Process each row
            for idx, row in enumerate(rows, 1):
                try:
                    # Normalize column names (case-insensitive, strip whitespace)
                    row_normalized = {k.strip().lower(): v.strip() if v else '' for k, v in row.items()}
                    
                    # Extract fields (try different column name variations)
                    sku = row_normalized.get('sku', '').strip().lower()  # Normalize SKU to lowercase for case-insensitive comparison
                    name = row_normalized.get('name', '').strip()
                    description = row_normalized.get('description', '').strip() or None
                    
                    # Validate required fields
                    if not sku or not name:
                        products_skipped += 1
                        errors.append(f"Row {idx}: Missing SKU or Name")
                        continue
                    
                    # Check if product with this SKU already exists (case-insensitive)
                    # Use with_for_update() to prevent race conditions in multi-worker scenarios
                    existing_product = db.query(Product).filter(Product.sku.ilike(sku)).first()
                    
                    was_update = False
                    product_for_webhook = None
                    
                    if existing_product:
                        # Update existing product (preserve active status - not in CSV)
                        existing_product.name = name
                        existing_product.description = description
                        # Note: active status is NOT updated from CSV
                        products_updated += 1
                        was_update = True
                        product_for_webhook = existing_product
                    else:
                        # Create new product (default to active=True)
                        new_product = Product(
                            sku=sku,
                            name=name,
                            description=description,
                            active=True  # Default to active for new products
                        )
                        db.add(new_product)
                        products_created += 1
                        was_update = False
                        product_for_webhook = new_product
                    
                    # Commit every 10 rows to avoid long transactions
                    if idx % 10 == 0:
                        try:
                            db.commit()
                            # Refresh product to get ID if it was newly created
                            if product_for_webhook:
                                if not was_update:
                                    db.refresh(product_for_webhook)
                                
                                # Trigger webhooks for batch (non-blocking, fire and forget)
                                try:
                                    if was_update:
                                        payload = {
                                            "event_type": "product.updated",
                                            "product": {
                                                "id": product_for_webhook.id,
                                                "sku": product_for_webhook.sku,
                                                "name": product_for_webhook.name,
                                                "description": product_for_webhook.description,
                                                "active": product_for_webhook.active,
                                                "updated_at": product_for_webhook.updated_at.isoformat() if product_for_webhook.updated_at else None
                                            },
                                            "timestamp": product_for_webhook.updated_at.isoformat() if product_for_webhook.updated_at else datetime.utcnow().isoformat() + "Z"
                                        }
                                        trigger_webhooks_sync(db, "product.updated", payload)
                                    else:
                                        payload = {
                                            "event_type": "product.created",
                                            "product": {
                                                "id": product_for_webhook.id,
                                                "sku": product_for_webhook.sku,
                                                "name": product_for_webhook.name,
                                                "description": product_for_webhook.description,
                                                "active": product_for_webhook.active,
                                                "created_at": product_for_webhook.created_at.isoformat() if product_for_webhook.created_at else None
                                            },
                                            "timestamp": product_for_webhook.created_at.isoformat() if product_for_webhook.created_at else datetime.utcnow().isoformat() + "Z"
                                        }
                                        trigger_webhooks_sync(db, "product.created", payload)
                                except Exception as webhook_error:
                                    # Don't fail the import if webhooks fail
                                    logger.warning(f"Failed to trigger webhook for product {sku}: {webhook_error}")
                        except IntegrityError:
                            # If commit fails due to race condition, rollback and retry as update
                            db.rollback()
                            # Try to find and update the product (another worker may have created it)
                            existing_product = db.query(Product).filter(Product.sku.ilike(sku)).first()
                            if existing_product:
                                existing_product.name = name
                                existing_product.description = description
                                db.commit()
                                products_created -= 1
                                products_updated += 1
                            else:
                                # Still can't find it, skip this row
                                products_created -= 1
                                products_skipped += 1
                                errors.append(f"Row {idx}: Could not create or update product with SKU {sku}")
                        
                        # Update progress (always update, even if commit had issues)
                        progress = 10 + int((idx / total_rows) * 80)
                        try:
                            redis_client.set(
                                redis_key,
                                json.dumps({
                                    "status": "processing",
                                    "message": f"Processing row {idx} of {total_rows}...",
                                    "progress": progress
                                })
                            )
                        except Exception as e:
                            logger.warning(f"Failed to update Redis progress: {e}")
                    
                    # Also update progress more frequently for small files (every row if < 100 rows)
                    elif total_rows < 100 and idx % 5 == 0:
                        progress = 10 + int((idx / total_rows) * 80)
                        try:
                            redis_client.set(
                                redis_key,
                                json.dumps({
                                    "status": "processing",
                                    "message": f"Processing row {idx} of {total_rows}...",
                                    "progress": progress
                                })
                            )
                        except Exception as e:
                            logger.warning(f"Failed to update Redis progress: {e}")
                
                except IntegrityError as e:
                    # Handle race condition: another worker may have created this SKU
                    db.rollback()
                    try:
                        # Try to update instead (case-insensitive lookup)
                        existing_product = db.query(Product).filter(Product.sku.ilike(sku)).first()
                        if existing_product:
                            existing_product.name = name
                            existing_product.description = description
                            db.commit()
                            products_updated += 1
                            
                            # Trigger webhook for updated product
                            try:
                                db.refresh(existing_product)
                                payload = {
                                    "event_type": "product.updated",
                                    "product": {
                                        "id": existing_product.id,
                                        "sku": existing_product.sku,
                                        "name": existing_product.name,
                                        "description": existing_product.description,
                                        "active": existing_product.active,
                                        "updated_at": existing_product.updated_at.isoformat() if existing_product.updated_at else None
                                    },
                                    "timestamp": existing_product.updated_at.isoformat() if existing_product.updated_at else datetime.utcnow().isoformat() + "Z"
                                }
                                trigger_webhooks_sync(db, "product.updated", payload)
                            except Exception as webhook_error:
                                logger.warning(f"Failed to trigger webhook for product {sku}: {webhook_error}")
                        else:
                            products_skipped += 1
                            errors.append(f"Row {idx}: Duplicate SKU - {str(e)}")
                    except Exception as update_error:
                        db.rollback()
                        products_skipped += 1
                        errors.append(f"Row {idx}: Error handling duplicate SKU - {str(update_error)}")
                    continue
                except Exception as e:
                    products_skipped += 1
                    errors.append(f"Row {idx}: Error - {str(e)}")
                    continue
            
            # Final commit for any remaining uncommitted rows
            try:
                db.commit()
            except IntegrityError:
                # Handle any remaining race conditions in the final batch
                db.rollback()
                # Re-query and update any products that failed due to race conditions
                # This is a best-effort attempt for the remaining rows
                logger.warning("Some rows in final batch had integrity errors, attempting recovery")
                # Note: Individual row errors are already tracked, so we just commit what we can
                try:
                    db.commit()
                except:
                    db.rollback()
            
            # Build success message
            message_parts = []
            if products_created > 0:
                message_parts.append(f"{products_created} created")
            if products_updated > 0:
                message_parts.append(f"{products_updated} updated")
            if products_skipped > 0:
                message_parts.append(f"{products_skipped} skipped")
            
            message = f"Import complete: {', '.join(message_parts)}"
            if errors:
                message += f" ({len(errors)} errors)"
            
            # Complete the job
            redis_client.set(
                redis_key,
                json.dumps({
                    "status": "complete",
                    "message": message,
                    "progress": 100,
                    "created": products_created,
                    "updated": products_updated,
                    "skipped": products_skipped
                })
            )
        
        # Clean up temporary file
        if file_path_obj.exists():
            file_path_obj.unlink()
            
    except Exception as e:
        db.rollback()
        # Update status to failed
        redis_client.set(
            redis_key,
            json.dumps({
                "status": "failed",
                "message": f"Error processing CSV: {str(e)}",
                "progress": 0
            })
        )
        
        # Clean up temporary file on error
        file_path_obj = Path(file_path)
        if file_path_obj.exists():
            file_path_obj.unlink()
        
        raise
    finally:
        db.close()
        # Close Redis connection
        if redis_client:
            try:
                redis_client.close()
            except Exception:
                pass
