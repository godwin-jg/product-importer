import asyncio
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.product import Product as ProductModel
from app.schemas.product import Product, ProductCreate, ProductUpdate
from app.services.webhook_service import trigger_webhooks_for_event

router = APIRouter(prefix="/products", tags=["products"])


@router.post("/", response_model=Product, status_code=201)
async def create_product(
    product: ProductCreate,
    db: Annotated[Session, Depends(get_db)]
):
    """Create a new product."""
    # Normalize SKU to lowercase for case-insensitive uniqueness
    normalized_sku = product.sku.lower().strip()
    
    # Check if SKU already exists (case-insensitive)
    existing_product = db.query(ProductModel).filter(ProductModel.sku.ilike(normalized_sku)).first()
    if existing_product:
        raise HTTPException(status_code=400, detail="Product with this SKU already exists")
    
    db_product = ProductModel(
        sku=normalized_sku,
        name=product.name,
        description=product.description,
        active=True
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    
    # Trigger webhooks asynchronously (don't wait for them)
    # Create a new session for webhook triggers to avoid session closure issues
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


@router.get("/", response_model=list[Product])
def list_products(
    db: Annotated[Session, Depends(get_db)],
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    sku: Annotated[str | None, Query()] = None,
    name: Annotated[str | None, Query()] = None,
    active: Annotated[bool | None, Query()] = None,
    description: Annotated[str | None, Query()] = None,
):
    """List products with pagination and filtering."""
    query = db.query(ProductModel)
    
    # Apply filters
    filters = []
    if sku is not None:
        # Normalize SKU filter to lowercase for case-insensitive search
        filters.append(ProductModel.sku.ilike(f"%{sku.lower().strip()}%"))
    if name is not None:
        filters.append(ProductModel.name.ilike(f"%{name}%"))
    if active is not None:
        filters.append(ProductModel.active == active)
    if description is not None:
        filters.append(ProductModel.description.ilike(f"%{description}%"))
    
    if filters:
        query = query.filter(and_(*filters))
    
    # Apply pagination
    products = query.offset(skip).limit(limit).all()
    return products


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
    
    # Store old values for webhook payload
    old_values = {
        "sku": db_product.sku,
        "name": db_product.name,
        "description": db_product.description,
        "active": db_product.active
    }
    
    # Check if SKU is being updated and if it already exists (case-insensitive)
    if product_update.sku is not None:
        normalized_sku = product_update.sku.lower().strip()
        # Compare case-insensitively
        if normalized_sku != db_product.sku.lower():
            existing_product = db.query(ProductModel).filter(
                ProductModel.sku.ilike(normalized_sku)
            ).first()
            if existing_product:
                raise HTTPException(status_code=400, detail="Product with this SKU already exists")
            db_product.sku = normalized_sku
    
    # Update other fields
    if product_update.name is not None:
        db_product.name = product_update.name
    if product_update.description is not None:
        db_product.description = product_update.description
    if product_update.active is not None:
        db_product.active = product_update.active
    
    db.commit()
    db.refresh(db_product)
    
    # Trigger webhooks asynchronously (don't wait for them)
    # Create a new session for webhook triggers to avoid session closure issues
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
    
    # Store product data for webhook before deletion
    product_data = {
        "id": db_product.id,
        "sku": db_product.sku,
        "name": db_product.name,
        "description": db_product.description,
        "active": db_product.active
    }
    
    db.delete(db_product)
    db.commit()
    
    # Trigger webhooks asynchronously (don't wait for them)
    # Create a new session for webhook triggers to avoid session closure issues
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

