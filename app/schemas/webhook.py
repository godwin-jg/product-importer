from pydantic import BaseModel


class WebhookBase(BaseModel):
    url: str
    event_type: str


class WebhookCreate(WebhookBase):
    pass


class Webhook(WebhookBase):
    id: int
    is_active: bool

    model_config = {"from_attributes": True}

