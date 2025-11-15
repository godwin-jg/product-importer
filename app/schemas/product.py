from datetime import datetime

from pydantic import BaseModel


class ProductBase(BaseModel):
    sku: str
    name: str
    description: str | None = None


class ProductCreate(ProductBase):
    pass


class ProductUpdate(ProductBase):
    sku: str | None = None
    name: str | None = None
    description: str | None = None
    active: bool | None = None


class Product(ProductBase):
    id: int
    active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = {"from_attributes": True}


class ProductListResponse(BaseModel):
    """Response model for paginated product list."""
    total: int | None  # None when count query times out (for large result sets)
    products: list[Product]

