import asyncio
import hashlib
import json
import logging
import re
import ssl
from datetime import datetime
from typing import Annotated

import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database import get_db
from app.models.product import Product as ProductModel
from app.schemas.product import Product, ProductCreate, ProductUpdate, ProductListResponse
from app.services.webhook_service import trigger_webhooks_for_event

logger = logging.getLogger(__name__)

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
    
    # Invalidate search cache since a new product was added
    _invalidate_search_cache()
    
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


def _get_cache_key(search: str | None, search_type: str | None, active: bool | None, skip: int, limit: int) -> str:
    """Generate a cache key for the search query."""
    cache_data = {
        "search": search,
        "search_type": search_type,
        "active": active,
        "skip": skip,
        "limit": limit
    }
    cache_str = json.dumps(cache_data, sort_keys=True)
    cache_hash = hashlib.md5(cache_str.encode()).hexdigest()
    return f"product_search:{cache_hash}"


def _get_redis_client():
    """Get a Redis client for caching."""
    try:
        return redis.from_url(
            settings.REDIS_URL,
            ssl_cert_reqs=ssl.CERT_NONE if "rediss" in settings.REDIS_URL else None
        )
    except Exception as e:
        logger.warning(f"Failed to create Redis client for caching: {e}")
        return None


def _invalidate_search_cache():
    """Invalidate all product search caches when products are modified."""
    redis_client = _get_redis_client()
    if redis_client:
        try:
            # Delete all keys matching the product_search pattern
            keys = redis_client.keys("product_search:*")
            if keys:
                redis_client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} search cache entries")
        except Exception as e:
            logger.warning(f"Cache invalidation error: {e}")
        finally:
            redis_client.close()


@router.get("/", response_model=ProductListResponse)
def list_products(
    db: Annotated[Session, Depends(get_db)],
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    search: Annotated[str | None, Query()] = None,
    search_type: Annotated[str | None, Query()] = None,  # "sku" or "text"
    active: Annotated[bool | None, Query()] = None,
):
    """
    Get paginated and filtered products.
    
    Optimized for 500k+ records:
    - Uses separate query paths for SKU vs full-text search
    - Fast timeout-protected COUNT (200ms) - returns null if slow
    - 5-second statement timeout for entire route
    - Redis caching for search results
    
    Args:
        search_type: "sku" for SKU search, "text" for name/description search. 
                     If not provided, defaults to "sku" for short terms, "text" for longer terms.
    """
    
    # --- Get Redis client once at the start ---
    redis_client = _get_redis_client()
    
    try:
        # Set a 5-second timeout for the entire route to protect the database
        try:
            db.execute(text("SET LOCAL statement_timeout = '5000'"))
        except Exception as e:
            logger.warning(f"Failed to set statement timeout: {e}")
        
        # Validate search_type if provided
        if search_type and search_type not in ["sku", "text"]:
            raise HTTPException(
                status_code=400, 
                detail="search_type must be 'sku' or 'text'"
            )
        
        # Only use cache when there's a search term (search queries are expensive and likely to be repeated)
        # For simple list queries without search, skip caching
        should_cache = bool(search and search.strip())
        cache_key = _get_cache_key(search, search_type, active, skip, limit) if should_cache else None
        
        if should_cache and redis_client:
            try:
                cached_result = redis_client.get(cache_key)
                if cached_result:
                    logger.info(f"Cache hit for key: {cache_key}")
                    cached_data = json.loads(cached_result.decode('utf-8'))
                    cached_products = [Product(**p) for p in cached_data.get("products", [])]
                    logger.info(f"Cache returned {len(cached_products)} products, total={cached_data.get('total')}")
                    return ProductListResponse(
                        total=cached_data.get("total"),
                        products=cached_products
                    )
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        elif not should_cache:
            logger.info("No search term provided, skipping cache")
        else:
            logger.info("Redis client not available, skipping cache")
        
        # --- CACHE MISS: Proceed with DB query ---
        logger.info(f"Query params: search={search}, search_type={search_type}, active={active}, skip={skip}, limit={limit}")
        
        query = db.query(ProductModel)
        
        if search:
            search_term = search.strip()
            
            # Require minimum 2 characters for search to avoid performance issues
            if len(search_term) < 2:
                # For very short searches, default to SKU prefix matching
                query = query.filter(ProductModel.sku.ilike(f"{search_term}%"))
                query = query.order_by(ProductModel.sku.asc())
            else:
                # Determine search type: use provided search_type or default to SKU
                if search_type:
                    # Use explicitly provided search type
                    use_sku_search = (search_type.lower() == "sku")
                else:
                    # Default to SKU search if not specified
                    use_sku_search = True
                
                if use_sku_search:
                    # --- SKU-ONLY PATH ---
                    # This is simple and will use the trigram index efficiently
                    logger.info(f"Using SKU search path for: {search_term}")
                    query = query.filter(ProductModel.sku.ilike(f"%{search_term}%"))
                    query = query.order_by(ProductModel.sku)  # Simple order
                else:
                    # --- FULL-TEXT-ONLY PATH ---
                    # This is simple and will use the GIN tsv index efficiently
                    logger.info(f"Using Full-Text search path for: {search_term}")
                    try:
                        tsquery = func.plainto_tsquery('english', search_term)
                        query = query.filter(ProductModel.tsv.op('@@')(tsquery))
                        query = query.order_by(
                            func.ts_rank(ProductModel.tsv, tsquery).desc()  # Rank only on this query
                        )
                    except Exception as e:
                        # Fallback for invalid text search
                        logger.warning(f"Full-text search failed: {e}")
                        query = query.filter(ProductModel.id == -1)  # Return no results
        else:
            # Default ordering when no search
            logger.info("No search term provided, using default ordering")
            query = query.order_by(ProductModel.id.desc())
        
        if active is not None:
            logger.info(f"Filtering by active status: {active}")
            query = query.filter(ProductModel.active == active)
        
        # --- OPTIMIZED COUNT: Fast timeout-protected COUNT(*) ---
        # For small datasets (< 1000 products), count should be fast, so use a reasonable timeout
        # For larger datasets, we'll return null total and use "Load More" pattern
        total_count = None
        try:
            # Use 1 second timeout for count (reasonable for most cases, but will timeout on huge datasets)
            db.execute(text("SET LOCAL statement_timeout = '1000'"))  # 1 second
            total_count = query.count()
            logger.info(f"Count succeeded: {total_count}")
        except Exception as e:
            # Count timed out or failed - this is OK for very large datasets
            # We'll return null total and the frontend will use "Load More" pattern
            logger.warning(f"Count timed out or failed, returning null total. Error: {e}")
        finally:
            # Always reset the timeout for the main query
            try:
                db.execute(text("SET LOCAL statement_timeout = '5000'"))  # Back to 5 seconds
            except Exception:
                pass
        
        # Now get the actual results with ordering and pagination
        logger.info(f"Fetching products with offset={skip}, limit={limit}")
        products = query.offset(skip).limit(limit).all()
        logger.info(f"Retrieved {len(products)} products from database")
        
        # Build response - convert to Pydantic models for proper serialization
        response = ProductListResponse(total=total_count, products=products)
        logger.info(f"Response: total={response.total}, products_count={len(response.products)}")
        
        # Only cache when there's a search term (search queries are expensive and likely to be repeated)
        if should_cache and redis_client and cache_key:
            try:
                # Serialize using Pydantic's model_dump for proper JSON serialization
                response_dict = response.model_dump()
                cache_value = json.dumps(response_dict, default=str)
                redis_client.setex(cache_key, 300, cache_value)  # 5 minutes TTL
                logger.info(f"Cached result for key: {cache_key}")
            except Exception as e:
                logger.warning(f"Cache write error: {e}")
        
        return response
    
    finally:
        # --- FIX: This 'finally' block will ALWAYS run ---
        # --- It ensures the client is closed on cache hits, misses, or errors ---
        if redis_client:
            try:
                redis_client.close()
                logger.debug("Redis connection closed")
            except Exception as e:
                logger.warning(f"Failed to close redis connection: {e}")


@router.delete("/all")
def delete_all_products(
    db: Annotated[Session, Depends(get_db)]
):
    """Delete all products."""
    db.query(ProductModel).delete()
    db.commit()
    
    # Invalidate search cache since all products were deleted
    _invalidate_search_cache()
    
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
    
    # Invalidate search cache since product was updated
    _invalidate_search_cache()
    
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
    
    # Invalidate search cache since product was deleted
    _invalidate_search_cache()
    
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


@router.post("/clear-cache")
def clear_cache():
    """Clear all product search cache entries."""
    _invalidate_search_cache()
    return {"ok": True, "message": "Cache cleared successfully"}

