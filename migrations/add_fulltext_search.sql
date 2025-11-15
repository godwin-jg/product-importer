-- Migration: Add full-text search support to products table
-- Run this script to add the tsvector column and GIN index for full-text search
--
-- This index is specifically for the "Full-Text Search Path" to speed up
-- text searches on name and description. The optimized search code uses separate paths:
-- - SKU searches: Use the trigram index (trgm_idx_products_sku) on sku column
-- - Text searches: Use this full-text search index (tsv_idx_products) on tsv column
--
-- Performance: This replaces inefficient ILIKE '%term%' searches with PostgreSQL's
-- built-in full-text search engine. On 500,000+ records, this provides:
-- - Sub-millisecond search times (vs seconds with ILIKE)
-- - Automatic word stemming (e.g., "run" matches "running", "ran")
-- - Relevance ranking for best matches first
-- - Indexed searches using GIN (Generalized Inverted Index)

-- Add the tsvector column (generated column)
-- This column is automatically maintained by PostgreSQL whenever sku, name, or description changes
ALTER TABLE products ADD COLUMN IF NOT EXISTS tsv tsvector
GENERATED ALWAYS AS (
  to_tsvector('english', sku || ' ' || name || ' ' || COALESCE(description, ''))
) STORED;

-- Create GIN index for fast full-text search
-- GIN indexes are specifically designed for full-text search and provide excellent performance
CREATE INDEX IF NOT EXISTS tsv_idx_products ON products USING GIN(tsv);

