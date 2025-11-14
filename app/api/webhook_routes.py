import time
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.webhook import Webhook as WebhookModel
from app.schemas.webhook import Webhook, WebhookCreate, WebhookUpdate

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/", response_model=Webhook, status_code=201)
def create_webhook(
    webhook: WebhookCreate,
    db: Annotated[Session, Depends(get_db)]
):
    """Create a new webhook."""
    db_webhook = WebhookModel(
        url=webhook.url,
        event_type=webhook.event_type,
        is_active=True
    )
    db.add(db_webhook)
    db.commit()
    db.refresh(db_webhook)
    return db_webhook


@router.get("/{webhook_id}", response_model=Webhook)
def get_webhook(
    webhook_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Get a webhook by ID."""
    webhook = db.query(WebhookModel).filter(WebhookModel.id == webhook_id).first()
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    return webhook


@router.get("/", response_model=list[Webhook])
def list_webhooks(
    db: Annotated[Session, Depends(get_db)]
):
    """List all webhooks."""
    webhooks = db.query(WebhookModel).all()
    return webhooks


@router.put("/{webhook_id}", response_model=Webhook)
def update_webhook(
    webhook_id: int,
    webhook_update: WebhookUpdate,
    db: Annotated[Session, Depends(get_db)]
):
    """Update a webhook."""
    db_webhook = db.query(WebhookModel).filter(WebhookModel.id == webhook_id).first()
    if not db_webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    # Update fields
    if webhook_update.url is not None:
        db_webhook.url = webhook_update.url
    if webhook_update.event_type is not None:
        db_webhook.event_type = webhook_update.event_type
    if webhook_update.is_active is not None:
        db_webhook.is_active = webhook_update.is_active
    
    db.commit()
    db.refresh(db_webhook)
    return db_webhook


@router.post("/{webhook_id}/test")
async def test_webhook(
    webhook_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Test a webhook by sending a sample payload."""
    webhook = db.query(WebhookModel).filter(WebhookModel.id == webhook_id).first()
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    if not webhook.is_active:
        raise HTTPException(status_code=400, detail="Webhook is not active")
    
    # Prepare test payload
    test_payload = {
        "event_type": webhook.event_type,
        "event": "test",
        "message": "This is a test webhook trigger",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            start_time = time.time()
            response = await client.post(
                webhook.url,
                json=test_payload,
                headers={"Content-Type": "application/json"}
            )
            end_time = time.time()
            response_time_ms = int((end_time - start_time) * 1000)
            
            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "response_time_ms": response_time_ms,
                "response_body": response.text[:500] if response.text else None,
                "message": f"Webhook responded with status {response.status_code}"
            }
    except httpx.TimeoutException:
        return {
            "success": False,
            "status_code": None,
            "response_time_ms": None,
            "response_body": None,
            "message": "Webhook request timed out after 10 seconds"
        }
    except Exception as e:
        return {
            "success": False,
            "status_code": None,
            "response_time_ms": None,
            "response_body": None,
            "message": f"Error testing webhook: {str(e)}"
        }


@router.delete("/{webhook_id}")
def delete_webhook(
    webhook_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Delete a webhook."""
    db_webhook = db.query(WebhookModel).filter(WebhookModel.id == webhook_id).first()
    if not db_webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    db.delete(db_webhook)
    db.commit()
    return {"ok": True}

