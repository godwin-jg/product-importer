import asyncio
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.product import Product as ProductModel
from app.schemas.product import Product, ProductCreate, ProductUpdate, ProductListResponse
from app.services.webhook_service import trigger_webhooks_for_event

router = APIRouter(prefix="/products", tags=["products"])


@router.post("/", response_model=Product, status_code=201)
async def create_product(
    product: ProductCreate,
    db: Annotated[Session, Depends(get_db)]
):
    """Create a new product."""
    normalized_sku = product.sku.lower().strip()
    existing_product = db.query(ProductModel).filter(ProductModel.sku.ilike(normalized_sku)).first()
    if existing_product:
        raise HTTPException(status_code=400, detail="Product with this SKU already exists")
    
    db_product = ProductModel(sku=normalized_sku, name=product.name, description=product.description, active=True)
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    from app.database import SessionLocal
    payload = {
        "event_type": "product.created",
        "product": {
            "id": db_product.id,
            "sku": db_product.sku,
            "name": db_product.name,
            "description": db_product.description,
            "active": db_product.active,
            "created_at": db_product.created_at.isoformat() if db_product.created_at else None
        },
        "timestamp": db_product.created_at.isoformat() if db_product.created_at else None
    }
    
    async def trigger_webhooks():
        webhook_db = SessionLocal()
        try:
            await trigger_webhooks_for_event(webhook_db, "product.created", payload)
        finally:
            webhook_db.close()
    
    asyncio.create_task(trigger_webhooks())
    return db_product


@router.get("/", response_model=ProductListResponse)
def list_products(
    db: Annotated[Session, Depends(get_db)],
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    search: Annotated[str | None, Query()] = None,
    active: Annotated[bool | None, Query()] = None,
):
    """Get paginated and filtered products."""
    query = db.query(ProductModel)
    
    if search:
        search_term = f"%{search.lower().strip()}%"
        query = query.filter(or_(
            func.lower(ProductModel.sku).ilike(search_term),
            func.lower(ProductModel.name).ilike(search_term)
        ))
    
    if active is not None:
        query = query.filter(ProductModel.active == active)
    
    total_count = query.count()
    products = query.offset(skip).limit(limit).all()
    
    return {"total": total_count, "products": products}


@router.delete("/all")
def delete_all_products(
    db: Annotated[Session, Depends(get_db)]
):
    """Delete all products."""
    db.query(ProductModel).delete()
    db.commit()
    return {"message": "All products deleted successfully", "ok": True}


@router.get("/{product_id}", response_model=Product)
def get_product(
    product_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Get a product by ID."""
    product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@router.put("/{product_id}", response_model=Product)
async def update_product(
    product_id: int,
    product_update: ProductUpdate,
    db: Annotated[Session, Depends(get_db)]
):
    """Update a product."""
    db_product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    old_values = {
        "sku": db_product.sku,
        "name": db_product.name,
        "description": db_product.description,
        "active": db_product.active
    }
    
    if product_update.sku is not None:
        normalized_sku = product_update.sku.lower().strip()
        if normalized_sku != db_product.sku.lower():
            existing_product = db.query(ProductModel).filter(ProductModel.sku.ilike(normalized_sku)).first()
            if existing_product:
                raise HTTPException(status_code=400, detail="Product with this SKU already exists")
            db_product.sku = normalized_sku
    
    if product_update.name is not None:
        db_product.name = product_update.name
    if product_update.description is not None:
        db_product.description = product_update.description
    if product_update.active is not None:
        db_product.active = product_update.active
    
    db.commit()
    db.refresh(db_product)
    
    from app.database import SessionLocal
    payload = {
        "event_type": "product.updated",
        "product": {
            "id": db_product.id,
            "sku": db_product.sku,
            "name": db_product.name,
            "description": db_product.description,
            "active": db_product.active,
            "updated_at": db_product.updated_at.isoformat() if db_product.updated_at else None
        },
        "old_values": old_values,
        "timestamp": db_product.updated_at.isoformat() if db_product.updated_at else None
    }
    
    async def trigger_webhooks():
        webhook_db = SessionLocal()
        try:
            await trigger_webhooks_for_event(webhook_db, "product.updated", payload)
        finally:
            webhook_db.close()
    
    asyncio.create_task(trigger_webhooks())
    return db_product


@router.delete("/{product_id}")
async def delete_product(
    product_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Delete a product."""
    db_product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    product_data = {
        "id": db_product.id,
        "sku": db_product.sku,
        "name": db_product.name,
        "description": db_product.description,
        "active": db_product.active
    }
    
    db.delete(db_product)
    db.commit()
    
    from app.database import SessionLocal
    payload = {
        "event_type": "product.deleted",
        "product": product_data,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    async def trigger_webhooks():
        webhook_db = SessionLocal()
        try:
            await trigger_webhooks_for_event(webhook_db, "product.deleted", payload)
        finally:
            webhook_db.close()
    
    asyncio.create_task(trigger_webhooks())
    return {"ok": True}

