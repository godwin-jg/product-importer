import os
import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException

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

