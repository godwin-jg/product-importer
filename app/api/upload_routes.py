import asyncio
import json
import ssl
import uuid
from pathlib import Path

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
    
    try:
        import redis
        redis_key = f"job:{job_id}"
        initial_redis_client = redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        initial_redis_client.set(
            redis_key,
            json.dumps({
                "status": "queued",
                "message": "File uploaded, queuing for processing...",
                "progress": 0
            })
        )
        initial_redis_client.close()
    except Exception:
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
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
        last_progress = -1
        
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
                        
                        if progress != last_progress or status in ["complete", "failed"]:
                            yield f"data: {redis_data_str}\n\n"
                            last_progress = progress
                        
                        if status in ["complete", "failed"]:
                            break
                    except json.JSONDecodeError:
                        yield f"data: {redis_data_str}\n\n"
                        if redis_data_str in ["complete", "failed"]:
                            break
                else:
                    yield f"data: {json.dumps({'status': 'pending', 'message': 'Job not found or not started'})}\n\n"
                
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

