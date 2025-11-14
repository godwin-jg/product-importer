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


class Product(ProductBase):
    id: int
    active: bool

    model_config = {"from_attributes": True}

