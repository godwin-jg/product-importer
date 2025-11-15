-- Migration: Add trigram index for fast SKU ILIKE queries
-- Run this script to enable pg_trgm extension and create GIN trigram index
--
-- This index is specifically for the "SKU Search Path" to speed up
-- ILIKE '%sku-code%' queries. The optimized search code uses separate paths:
-- - SKU searches: Use this trigram index on sku column
-- - Text searches: Use the full-text search index (tsv_idx_products) on tsv column
--
-- Performance: The pg_trgm extension breaks text into 3-character chunks and indexes them
-- This allows PostgreSQL to use the index for pattern matching queries, even with leading %
--
-- Benefits:
-- - Fast ILIKE '%term%' queries on SKU column (can use index)
-- - Complements full-text search for different use cases
-- - Minimal index overhead (only one trigram index needed)

-- Enable the pg_trgm extension (run once per database)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create GIN trigram index ONLY on the sku column
-- This is all that's needed to support the SKU search path
-- Using CONCURRENTLY to avoid locking the table during index creation
-- Note: CONCURRENTLY cannot be used inside a transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS trgm_idx_products_sku 
ON products 
USING GIN (sku gin_trgm_ops);

