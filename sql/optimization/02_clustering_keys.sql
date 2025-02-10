-- ============================================================================
-- Clustering Keys for Query Performance
-- ============================================================================
-- Purpose: Physically organize data for faster filtering and pruning
--
-- What is Clustering?
-- - Snowflake stores data in micro-partitions (50-500 MB compressed chunks)
-- - Clustering organizes rows within micro-partitions by specified columns
-- - Enables "partition pruning" - skipping irrelevant micro-partitions
--
-- When to Use Clustering:
-- - Large tables (multi-TB in production, but demonstrates concept here)
-- - Columns frequently used in WHERE clauses (date, region, status)
-- - High-cardinality columns (many distinct values)
-- - Queries that filter on consistent columns
--
-- Best Practices:
-- - Start with date columns (most common filter in analytics)
-- - Add 1-2 additional high-cardinality columns if needed
-- - More clustering keys = more maintenance cost
-- - Monitor clustering depth (lower = better organized)
--
-- Cost: Background compute for automatic re-clustering as data changes
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Add Clustering Key to Fact Table
-- ============================================================================
-- Clustering on: (date_key, country_key)
-- Rationale:
--   1. date_key: Most queries filter by date range (last month, YTD, etc.)
--   2. country_key: Common geographic segmentation in analysis
--
-- Expected Benefit:
-- - Queries with "WHERE date_key BETWEEN X AND Y" skip irrelevant partitions
-- - Queries with "WHERE country_key = X" benefit from co-location
-- - Combined filters get maximum pruning benefit
--
-- Note: For this small dataset (~500K rows), benefits are minimal.
--       In production with billions of rows, pruning can eliminate 90%+ scans.
-- ============================================================================

ALTER TABLE fact_sales
  CLUSTER BY (date_key, country_key);

-- ============================================================================
-- Check Clustering Information
-- ============================================================================
-- Returns JSON with clustering statistics:
-- - cluster_by_keys: Columns used for clustering
-- - total_micro_partitions: Number of micro-partitions in table
-- - average_overlaps: How many partitions contain same key values (lower = better)
-- - average_depth: How deep to scan to find all data for a key (lower = better)
-- ============================================================================

SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales');

-- ============================================================================
-- View Clustering Depth
-- ============================================================================
-- Clustering depth measures data organization quality:
-- - Depth = 1: Perfect clustering (each key value in single partition)
-- - Depth = N: Must scan N partitions on average to find all data for a key
-- - Lower depth = better query performance
--
-- Target: Aim for depth < 5 for production tables
-- ============================================================================

SELECT SYSTEM$CLUSTERING_DEPTH('fact_sales', '(date_key, country_key)');

-- ============================================================================
-- Usage Notes
-- ============================================================================
-- 1. Automatic Clustering:
--    - Snowflake's Automatic Clustering service maintains clustering
--    - Runs in background, no manual intervention needed
--    - Consumes compute credits for maintenance
--
-- 2. Monitoring:
--    - Re-run clustering depth queries periodically
--    - If depth increases significantly, re-clustering may be needed
--    - Check ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY for costs
--
-- 3. When to Remove Clustering:
--    - If query patterns change (different WHERE clauses used)
--    - If maintenance costs exceed query savings
--    - Use ALTER TABLE ... SUSPEND RECLUSTER to pause maintenance
--
-- 4. Alternative Strategies:
--    - Partition by date externally before loading (pre-cluster in ETL)
--    - Use search optimization for point lookups instead
--    - Denormalize to reduce joins (sometimes better than clustering)
-- ============================================================================
