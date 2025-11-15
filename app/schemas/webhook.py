from datetime import datetime
from urllib.parse import urlparse

from pydantic import BaseModel, field_validator


class WebhookBase(BaseModel):
    url: str
    event_type: str
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate that the URL is well-formed."""
        if not v:
            raise ValueError("URL cannot be empty")
        
        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("URL must include a scheme (http:// or https://) and a hostname")
        
        if parsed.scheme not in ("http", "https"):
            raise ValueError("URL scheme must be http:// or https://")
        
        return v
    
    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type."""
        if not v:
            raise ValueError("Event type cannot be empty")
        
        valid_event_types = (
            "product.created", 
            "product.updated", 
            "product.deleted",
            "import.complete",
            "import.failed"
        )
        if v not in valid_event_types:
            raise ValueError(f"Event type must be one of: {', '.join(valid_event_types)}")
        
        return v


class WebhookCreate(WebhookBase):
    pass


class WebhookUpdate(BaseModel):
    url: str | None = None
    event_type: str | None = None
    is_active: bool | None = None

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str | None) -> str | None:
        """Reuse the validation logic from WebhookBase, but allow None."""
        if v is None:
            return None
        return WebhookBase.validate_url(v)  # Reuse the validator

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str | None) -> str | None:
        """Reuse the validation logic from WebhookBase, but allow None."""
        if v is None:
            return None
        return WebhookBase.validate_event_type(v)  # Reuse the validator


class Webhook(WebhookBase):
    id: int
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}

