import asyncio
import json
import ssl
import uuid
import httpx  # For downloading the file in the importer

import redis.asyncio as aioredis  # Use async redis
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.config import settings


router = APIRouter(prefix="/upload", tags=["upload"])


class CompleteUploadRequest(BaseModel):
    """Request model for completing CSV upload."""
    job_id: str
    file_url: str  # The blob URL returned by Vercel Blob


@router.post("/csv/init")
async def init_csv_upload():
    """
    Initialize a CSV upload by generating a job_id and Vercel Blob upload URL.
    This allows direct client-side upload to Vercel Blob, bypassing Vercel's size limits.
    """
    job_id = str(uuid.uuid4())
    
    # Use aioredis for async operation
    redis_client = None
    try:
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None,
            decode_responses=True
        )
        await redis_client.set(f"job:{job_id}", json.dumps({
            "status": "uploading",
            "message": "Waiting for file upload...",
            "progress": 0
        }))
    except Exception as e:
        # Log the error but don't fail the upload init
        print(f"Redis error in /init: {e}")
    finally:
        if redis_client:
            await redis_client.aclose()
    
    # Check if Vercel Blob is configured
    if not settings.BLOB_READ_WRITE_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Vercel Blob is not configured. Large file uploads require BLOB_READ_WRITE_TOKEN environment variable."
        )

    try:
        # Generate a unique filename for the blob
        filename = f"csv_imports/{job_id}.csv"
        
        # Return the token and filename for client-side upload
        # The frontend will upload directly to Vercel Blob
        # Note: In production, consider using a more secure approach like
        # generating temporary tokens or using serverless functions
        # The pathname goes in the URL path to avoid CORS issues with custom headers
        return {
            "job_id": job_id,
            "blob_token": settings.BLOB_READ_WRITE_TOKEN,
            "filename": filename,
            "upload_endpoint": f"https://blob.vercel-storage.com/put/{filename}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize upload: {str(e)}")


@router.post("/csv/complete")
async def complete_csv_upload(request: CompleteUploadRequest):
    """
    Complete the CSV upload process after file has been uploaded to Vercel Blob.
    This endpoint is called after the client successfully uploads to Vercel Blob.
    """
    job_id = request.job_id
    file_url = request.file_url
    
    # Use aioredis for async operations
    redis_client = None
    try:
        redis_client = aioredis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None,
            decode_responses=True
        )
        
        # Validate job exists
        job_data = await redis_client.get(f"job:{job_id}")
        if not job_data:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Update status
        await redis_client.set(f"job:{job_id}", json.dumps({
            "status": "queued",
            "message": "File uploaded to cloud, queuing for processing...",
            "progress": 0
        }))
        
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        # Log the error but continue to queue the task
        print(f"Redis error in /complete: {e}")
    finally:
        if redis_client:
            await redis_client.aclose()
    
    # Start processing
    try:
        from app.services.importer import process_csv_import
        process_csv_import.delay(file_url, job_id)
        
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
    """
    DEPRECATED: This endpoint is disabled due to Vercel's 4.5MB request body limit.
    """
    raise HTTPException(
        status_code=410,  # 410 Gone is the correct code for a permanently deprecated endpoint
        detail=(
            "This endpoint is deprecated and disabled. "
            "Files larger than 4.5MB will fail on Vercel. "
            "Please use the new upload flow: POST /upload/csv/init, "
            "upload to Vercel Blob, then POST /upload/csv/complete."
        )
    )


@router.get("/progress/{job_id}")
async def upload_progress(job_id: str):
    """
    Stream upload progress using Server-Sent Events.
    (This endpoint is already using aioredis and is well-implemented)
    """
    async def event_generator():
        redis_client = None
        try:
            redis_client = aioredis.from_url(
                settings.REDIS_URL,
                ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None,
                decode_responses=True
            )
            
            last_progress = -1
            last_status = None
            last_message = None
            
            # Send initial status immediately
            try:
                initial_data = await redis_client.get(f"job:{job_id}")
                if initial_data:
                    initial_json = json.loads(initial_data)
                    yield f"data: {initial_data}\n\n"
                    last_progress = initial_json.get("progress", 0)
                    last_status = initial_json.get("status", "unknown")
                    last_message = initial_json.get("message", "")
            except Exception:
                pass
            
            while True:
                redis_data = await redis_client.get(f"job:{job_id}")
                
                if redis_data:
                    try:
                        data = json.loads(redis_data)
                        status = data.get("status", "unknown")
                        progress = data.get("progress", 0)
                        message = data.get("message", "")
                        
                        # Send update if data changed
                        if (progress != last_progress or 
                            status != last_status or 
                            message != last_message):
                            
                            yield f"data: {redis_data}\n\n"
                            last_progress = progress
                            last_status = status
                            last_message = message
                        
                        if status in ["complete", "failed"]:
                            break
                    except json.JSONDecodeError:
                        yield f"data: {json.dumps({'status': 'error', 'message': 'Invalid job data'})}\n\n"
                else:
                    # Job key might have been evicted or not set yet
                    yield f"data: {json.dumps({'status': last_status or 'uploading', 'message': last_message or 'Waiting for job...', 'progress': last_progress or 0})}\n\n"

                await asyncio.sleep(0.5)  # Poll slightly slower to reduce Redis load
        
        except asyncio.CancelledError:
            # Client disconnected
            raise
        except Exception as e:
            print(f"SSE Error: {e}")
            yield f"data: {json.dumps({'status': 'failed', 'message': 'Error streaming progress'})}\n\n"
        finally:
            if redis_client:
                await redis_client.aclose()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )
