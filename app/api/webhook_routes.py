from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.webhook import Webhook as WebhookModel
from app.schemas.webhook import Webhook, WebhookCreate

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

