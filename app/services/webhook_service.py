import asyncio
import logging
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy.orm import Session

from app.models.webhook import Webhook as WebhookModel

logger = logging.getLogger(__name__)


async def trigger_webhook(webhook: WebhookModel, event_type: str, payload: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
    """Trigger a webhook asynchronously."""
    if not webhook.is_active:
        return {"success": False, "message": "Webhook is not active", "webhook_id": webhook.id}
    if webhook.event_type != event_type:
        return {"success": False, "message": f"Event type mismatch: {webhook.event_type} != {event_type}", "webhook_id": webhook.id}
    
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(webhook.url, json=payload, headers={"Content-Type": "application/json"})
            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "webhook_id": webhook.id,
                "url": webhook.url,
                "response_body": response.text[:500] if response.text else None
            }
    except httpx.TimeoutException:
        logger.warning(f"Webhook {webhook.id} timed out")
        return {"success": False, "status_code": None, "webhook_id": webhook.id, "url": webhook.url, "message": f"Timeout after {timeout}s"}
    except Exception as e:
        logger.error(f"Error triggering webhook {webhook.id}: {e}")
        return {"success": False, "status_code": None, "webhook_id": webhook.id, "url": webhook.url, "message": str(e)}


async def trigger_webhooks_for_event(db: Session, event_type: str, payload: dict[str, Any], timeout: float = 10.0) -> list[dict[str, Any]]:
    """Trigger all active webhooks for a given event type."""
    webhooks = db.query(WebhookModel).filter(WebhookModel.event_type == event_type, WebhookModel.is_active == True).all()
    if not webhooks:
        return []
    
    results = await asyncio.gather(*[trigger_webhook(w, event_type, payload, timeout) for w in webhooks], return_exceptions=True)
    
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results.append({"success": False, "webhook_id": webhooks[i].id, "url": webhooks[i].url, "message": str(result)})
        else:
            processed_results.append(result)
    
    return processed_results


def trigger_webhooks_sync(db: Session, event_type: str, payload: dict[str, Any], timeout: float = 10.0) -> list[dict[str, Any]]:
    """Synchronous wrapper for trigger_webhooks_for_event."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(trigger_webhooks_for_event(db, event_type, payload, timeout))

