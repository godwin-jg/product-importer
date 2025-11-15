import asyncio
import base64
import io
import json
import ssl
import uuid
from pathlib import Path

import cloudinary
import cloudinary.uploader
import redis.asyncio as aioredis
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from app.core.config import settings

# Configure Cloudinary if credentials are provided
if settings.CLOUDINARY_CLOUD_NAME and settings.CLOUDINARY_API_KEY and settings.CLOUDINARY_API_SECRET:
    cloudinary.config(
        cloud_name=settings.CLOUDINARY_CLOUD_NAME,
        api_key=settings.CLOUDINARY_API_KEY,
        api_secret=settings.CLOUDINARY_API_SECRET
    )

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/csv/init")
async def init_csv_upload():
    """
    Initialize a CSV upload by generating a job_id and Cloudinary upload signature.
    This allows direct client-side upload to Cloudinary, bypassing Vercel's size limits.
    """
    import hashlib
    import time
    
    # Generate unique job_id
    job_id = uuid.uuid4()
    
    # Initialize job status in Redis
    try:
        import redis
        client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        client.set(f"job:{job_id}", json.dumps({
            "status": "uploading",
            "message": "Waiting for file upload...",
            "progress": 0
        }))
        client.close()
    except Exception:
        pass
    
    # If Cloudinary is configured, generate upload signature for direct client upload
    if settings.CLOUDINARY_CLOUD_NAME and settings.CLOUDINARY_API_KEY and settings.CLOUDINARY_API_SECRET:
        try:
            # Generate Cloudinary upload signature
            timestamp = int(time.time())
            public_id = f"csv_imports/{job_id}"
            
            # Create signature string (parameters sorted alphabetically, then append secret)
            params = {
                'folder': 'csv_imports',
                'public_id': public_id,
                'timestamp': str(timestamp)
            }
            # Sort parameters and create signature string
            param_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
            signature_string = param_string + settings.CLOUDINARY_API_SECRET
            signature = hashlib.sha1(signature_string.encode('utf-8')).hexdigest()
            
            return {
                "job_id": str(job_id),
                "cloudinary": {
                    "cloud_name": settings.CLOUDINARY_CLOUD_NAME,
                    "api_key": settings.CLOUDINARY_API_KEY,
                    "timestamp": timestamp,
                    "signature": signature,
                    "folder": "csv_imports",
                    "public_id": public_id,
                    "upload_url": f"https://api.cloudinary.com/v1_1/{settings.CLOUDINARY_CLOUD_NAME}/raw/upload"
                }
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to initialize upload: {str(e)}")
    else:
        # Fallback: return job_id for server-side upload (will hit size limits on Vercel)
        return {
            "job_id": str(job_id),
            "cloudinary": None
        }


@router.post("/csv/complete")
async def complete_csv_upload(job_id: str, file_url: str):
    """
    Complete the CSV upload process after file has been uploaded to Cloudinary.
    This endpoint is called after the client successfully uploads to Cloudinary.
    """
    # Validate job exists
    try:
        import redis
        client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        job_data = client.get(f"job:{job_id}")
        if not job_data:
            raise HTTPException(status_code=404, detail="Job not found")
        client.close()
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        pass
    
    # Update status
    try:
        import redis
        client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        client.set(f"job:{job_id}", json.dumps({
            "status": "queued",
            "message": "File uploaded to cloud, queuing for processing...",
            "progress": 0
        }))
        client.close()
    except Exception:
        pass
    
    # Start processing
    try:
        from app.services.importer import process_csv_import
        process_csv_import.delay(file_url, job_id, use_cloudinary=True)
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Import task not available. Please ensure the task is defined."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")
    
    return {"job_id": job_id, "message": "File uploaded and processing started"}


@router.post("/csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a CSV file for processing."""
    # Validate file extension
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    # Generate unique job_id
    job_id = uuid.uuid4()
    
    # Initialize job status in Redis
    try:
        import redis
        client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        client.set(f"job:{job_id}", json.dumps({
            "status": "queued",
            "message": "Uploading file...",
            "progress": 0
        }))
        client.close()
    except Exception:
        pass
    
    # Upload to Cloudinary if configured, otherwise use base64 fallback
    file_url = None
    file_content_b64 = None
    
    if settings.CLOUDINARY_CLOUD_NAME and settings.CLOUDINARY_API_KEY and settings.CLOUDINARY_API_SECRET:
        # Upload to Cloudinary for large files
        try:
            # Read file content
            file_content = await file.read()
            
            # Upload to Cloudinary with a unique public_id
            # Use BytesIO to create a file-like object for Cloudinary
            file_like = io.BytesIO(file_content)
            upload_result = cloudinary.uploader.upload(
                file_like,
                resource_type="raw",
                public_id=f"csv_imports/{job_id}",
                folder="csv_imports",
                overwrite=True,
                use_filename=False
            )
            file_url = upload_result.get("secure_url") or upload_result.get("url")
            
            # Update status
            try:
                import redis
                client = redis.from_url(
                    settings.REDIS_URL,
                    ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
                )
                client.set(f"job:{job_id}", json.dumps({
                    "status": "queued",
                    "message": "File uploaded to cloud, queuing for processing...",
                    "progress": 0
                }))
                client.close()
            except Exception:
                pass
                
        except Exception as e:
            # Fall back to base64 if Cloudinary upload fails
            await file.seek(0)
            file_content = await file.read()
            file_content_b64 = base64.b64encode(file_content).decode('utf-8')
    else:
        # Fallback to base64 encoding if Cloudinary is not configured
        try:
            file_content = await file.read()
            file_content_b64 = base64.b64encode(file_content).decode('utf-8')
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read file: {str(e)}")
    
    # Import and call the Celery task
    try:
        from app.services.importer import process_csv_import
        if file_url:
            # Pass Cloudinary URL to task
            process_csv_import.delay(file_url, str(job_id), use_cloudinary=True)
        else:
            # Pass base64 content (fallback)
            process_csv_import.delay(file_content_b64, str(job_id), use_cloudinary=False)
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Import task not available. Please ensure the task is defined."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")
    
    return {"job_id": str(job_id), "message": "File uploaded and processing started"}


@router.get("/progress/{job_id}")
async def upload_progress(job_id: str):
    """Stream upload progress using Server-Sent Events."""
    async def event_generator():
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        last_progress = -1
        last_status = None
        last_message = None
        
        # Send initial status immediately
        try:
            initial_data = await redis_client.get(f"job:{job_id}")
            if initial_data:
                initial_data_str = initial_data.decode('utf-8') if isinstance(initial_data, bytes) else str(initial_data)
                try:
                    initial_json = json.loads(initial_data_str)
                    yield f"data: {initial_data_str}\n\n"
                    last_progress = initial_json.get("progress", 0)
                    last_status = initial_json.get("status", "unknown")
                    last_message = initial_json.get("message", "")
                except json.JSONDecodeError:
                    yield f"data: {initial_data_str}\n\n"
        except Exception:
            pass
        
        try:
            while True:
                # Check Redis for job status
                redis_key = f"job:{job_id}"
                redis_data = await redis_client.get(redis_key)
                
                if redis_data:
                    redis_data_str = redis_data.decode('utf-8') if isinstance(redis_data, bytes) else str(redis_data)
                    try:
                        data = json.loads(redis_data_str)
                        status = data.get("status", "unknown")
                        progress = data.get("progress", 0)
                        message = data.get("message", "")
                        
                        # Send update if progress, status, or message changed
                        progress_changed = progress != last_progress
                        status_changed = status != last_status
                        message_changed = message != last_message
                        
                        if progress_changed or status_changed or message_changed or status in ["complete", "failed"]:
                            yield f"data: {redis_data_str}\n\n"
                            last_progress = progress
                            last_status = status
                            last_message = message
                        
                        if status in ["complete", "failed"]:
                            break
                    except json.JSONDecodeError:
                        yield f"data: {redis_data_str}\n\n"
                        if redis_data_str in ["complete", "failed"]:
                            break
                else:
                    # Job not found - send last known status
                    if last_status:
                        yield f"data: {json.dumps({'status': last_status, 'message': last_message, 'progress': last_progress})}\n\n"
                
                await asyncio.sleep(0.3)
        
        finally:
            await redis_client.aclose()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable buffering for nginx
        }
    )

