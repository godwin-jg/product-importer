-- Cleanup: Drop redundant trigram indexes
-- Run this script if you previously ran the old migration that created multiple trigram indexes
--
-- After optimizing the search code to use separate SKU and Full-Text search paths,
-- we only need:
-- - trgm_idx_products_sku (for SKU searches)
-- - tsv_idx_products (for full-text searches, created by add_fulltext_search.sql)
--
-- These indexes are now redundant and can be safely dropped:
-- - trgm_idx_products_name (name searches now use full-text search)
-- - trgm_idx_products_sku_name (combined searches now use full-text search)

-- Drop redundant trigram index on name column
-- (Name searches now use the full-text search index on tsv)
DROP INDEX CONCURRENTLY IF EXISTS trgm_idx_products_name;

-- Drop redundant combined trigram index
-- (Combined searches now use the full-text search index on tsv)
DROP INDEX CONCURRENTLY IF EXISTS trgm_idx_products_sku_name;

-- Note: We keep trgm_idx_products_sku as it's used by the SKU search path

