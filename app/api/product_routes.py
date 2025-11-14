from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.product import Product as ProductModel
from app.schemas.product import Product, ProductCreate, ProductUpdate

router = APIRouter(prefix="/products", tags=["products"])


@router.post("/", response_model=Product, status_code=201)
def create_product(
    product: ProductCreate,
    db: Annotated[Session, Depends(get_db)]
):
    """Create a new product."""
    # Check if SKU already exists
    existing_product = db.query(ProductModel).filter(ProductModel.sku == product.sku).first()
    if existing_product:
        raise HTTPException(status_code=400, detail="Product with this SKU already exists")
    
    db_product = ProductModel(
        sku=product.sku,
        name=product.name,
        description=product.description,
        active=True
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product


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
        filters.append(ProductModel.sku.ilike(f"%{sku}%"))
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


@router.put("/{product_id}", response_model=Product)
def update_product(
    product_id: int,
    product_update: ProductUpdate,
    db: Annotated[Session, Depends(get_db)]
):
    """Update a product."""
    db_product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Check if SKU is being updated and if it already exists
    if product_update.sku is not None and product_update.sku != db_product.sku:
        existing_product = db.query(ProductModel).filter(
            ProductModel.sku == product_update.sku
        ).first()
        if existing_product:
            raise HTTPException(status_code=400, detail="Product with this SKU already exists")
        db_product.sku = product_update.sku
    
    # Update other fields
    if product_update.name is not None:
        db_product.name = product_update.name
    if product_update.description is not None:
        db_product.description = product_update.description
    if product_update.active is not None:
        db_product.active = product_update.active
    
    db.commit()
    db.refresh(db_product)
    return db_product


@router.delete("/{product_id}")
def delete_product(
    product_id: int,
    db: Annotated[Session, Depends(get_db)]
):
    """Delete a product."""
    db_product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    db.delete(db_product)
    db.commit()
    return {"ok": True}


@router.delete("/all")
def delete_all_products(
    db: Annotated[Session, Depends(get_db)]
):
    """Delete all products."""
    db.query(ProductModel).delete()
    db.commit()
    return {"message": "All products deleted successfully", "ok": True}

