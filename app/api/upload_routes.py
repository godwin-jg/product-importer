import asyncio
import json
import os
import ssl
import uuid
from pathlib import Path
from urllib.parse import urlparse

import redis
import redis.asyncio as aioredis
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from app.core.config import settings

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a CSV file for processing."""
    # Validate file extension
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    # Generate unique job_id
    job_id = uuid.uuid4()
    
    # Create temporary file path
    temp_dir = Path("/tmp")
    temp_file_path = temp_dir / f"{job_id}.csv"
    
    # Ensure temp directory exists
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Save uploaded file to temporary location
    try:
        with open(temp_file_path, "wb") as f:
            content = await file.read()
            f.write(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
    
    # Set initial status in Redis immediately (before task starts)
    try:
        import redis
        redis_url_parsed = urlparse(settings.REDIS_URL)
        is_ssl = redis_url_parsed.scheme == "rediss"
        
        if is_ssl:
            import ssl
            initial_redis_client = redis.from_url(
                settings.REDIS_URL,
                ssl_cert_reqs=ssl.CERT_NONE
            )
        else:
            initial_redis_client = redis.from_url(settings.REDIS_URL)
        
        redis_key = f"job:{job_id}"
        initial_redis_client.set(
            redis_key,
            json.dumps({
                "status": "queued",
                "message": "File uploaded, queuing for processing...",
                "progress": 5
            })
        )
        initial_redis_client.close()
    except Exception as e:
        # If Redis update fails, continue anyway - task will update it
        pass
    
    # Import and call the Celery task
    try:
        from app.services.importer import process_csv_import
        process_csv_import.delay(str(temp_file_path), str(job_id))
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="Import task not available. Please ensure the task is defined."
        )
    except Exception as e:
        # Clean up file if task submission fails
        if temp_file_path.exists():
            temp_file_path.unlink()
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")
    
    return {"job_id": str(job_id), "message": "File uploaded and processing started"}


@router.get("/progress/{job_id}")
async def upload_progress(job_id: str):
    """Stream upload progress using Server-Sent Events."""
    async def event_generator():
        # Create async Redis client with SSL support if needed
        redis_url_parsed = urlparse(settings.REDIS_URL)
        is_ssl = redis_url_parsed.scheme == "rediss"
        
        if is_ssl:
            redis_client = aioredis.from_url(
                settings.REDIS_URL,
                ssl_cert_reqs=ssl.CERT_NONE
            )
        else:
            redis_client = aioredis.from_url(settings.REDIS_URL)
        
        last_progress = -1  # Track last progress to avoid duplicate updates
        
        try:
            while True:
                # Check Redis for job status
                redis_key = f"job:{job_id}"
                redis_data = await redis_client.get(redis_key)
                
                if redis_data:
                    # Decode bytes to string if needed
                    if isinstance(redis_data, bytes):
                        redis_data_str = redis_data.decode('utf-8')
                    else:
                        redis_data_str = str(redis_data)
                    
                    # Parse the data (assuming it's JSON)
                    try:
                        data = json.loads(redis_data_str)
                        status = data.get("status", "unknown")
                        progress = data.get("progress", 0)
                        
                        # Only send if progress changed (avoid duplicate updates)
                        if progress != last_progress or status in ["complete", "failed"]:
                            # Yield in SSE format
                            yield f"data: {redis_data_str}\n\n"
                            last_progress = progress
                        
                        # Break if job is complete or failed
                        if status in ["complete", "failed"]:
                            break
                    except json.JSONDecodeError:
                        # If not JSON, send as-is
                        yield f"data: {redis_data_str}\n\n"
                        if redis_data_str in ["complete", "failed"]:
                            break
                else:
                    # Job not found or not started yet
                    yield f"data: {json.dumps({'status': 'pending', 'message': 'Job not found or not started'})}\n\n"
                
                # Wait before next check (poll more frequently for better responsiveness)
                await asyncio.sleep(0.5)
        
        finally:
            # Close Redis connection
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

