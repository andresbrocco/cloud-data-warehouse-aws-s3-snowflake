-- ============================================================================
-- Search Optimization Service
-- ============================================================================
-- Purpose: Accelerate point lookup queries with equality predicates
--
-- What is Search Optimization?
-- - Creates and maintains search access paths (similar to indexes)
-- - Optimizes queries with WHERE col = 'value' or WHERE col IN (...)
-- - Different from clustering (clustering = range scans, search = point lookups)
--
-- When to Use:
-- - High-cardinality columns (customer_id, invoice_no, stock_code, email)
-- - Frequent equality lookups (find specific customer/order/product)
-- - Queries with selective filters (return small % of rows)
--
-- When NOT to Use:
-- - Range queries (use clustering instead)
-- - Low-cardinality columns (category, status - not selective enough)
-- - Rarely-queried columns (cost > benefit)
--
-- Cost:
-- - Storage: Search access structures consume additional space
-- - Compute: Background maintenance as data changes
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Enable Search Optimization on Customer Dimension
-- ============================================================================
-- Use Case: Lookup customer by customer_id (e.g., "Show me customer 12345")
-- Benefit: Fast point lookup without scanning entire dimension table
-- Typical Query: SELECT * FROM dim_customer WHERE customer_id = '12345'
-- ============================================================================

ALTER TABLE dim_customer
  ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id);

-- ============================================================================
-- Enable Search Optimization on Product Dimension
-- ============================================================================
-- Use Case: Lookup product by stock_code (e.g., "Show me product details for SKU ABC")
-- Benefit: Fast product lookups for inventory, pricing, and catalog queries
-- Typical Query: SELECT * FROM dim_product WHERE stock_code = '85123A'
-- ============================================================================

ALTER TABLE dim_product
  ADD SEARCH OPTIMIZATION ON EQUALITY(stock_code);

-- ============================================================================
-- Check Search Optimization Status
-- ============================================================================
-- Shows all tables with search optimization enabled:
-- - Table name
-- - Search method (ON EQUALITY, ON SUBSTRING, etc.)
-- - Columns optimized
-- - Maintenance status
-- ============================================================================

SHOW SEARCH OPTIMIZATION IN SCHEMA ECOMMERCE_DW.PRODUCTION;

-- ============================================================================
-- Usage Notes
-- ============================================================================
-- 1. Automatic Maintenance:
--    - Snowflake maintains search structures automatically
--    - Background process updates as table data changes
--    - No manual index rebuilds required
--
-- 2. Query Patterns:
--    - Works best with: WHERE col = value, WHERE col IN (values)
--    - Also supports: SUBSTRING search (for text columns)
--    - Does NOT help: Range queries (>, <, BETWEEN), full scans
--
-- 3. Monitoring:
--    - Check query profile in Snowflake UI (shows if search opt used)
--    - Compare execution time before/after enabling
--    - Monitor SEARCH_OPTIMIZATION_HISTORY for costs
--
-- 4. Cost vs Benefit:
--    - Typically worth it for: dim_customer, dim_product, fact foreign keys
--    - Questionable for: Low-cardinality columns, infrequently queried tables
--    - Disable if costs exceed savings: ALTER TABLE ... DROP SEARCH OPTIMIZATION
--
-- 5. Combining Optimizations:
--    - Clustering + Search Optimization: Use both for different query patterns
--    - Clustering for date ranges, search opt for ID lookups
--    - Materialized views can also benefit from search optimization
-- ============================================================================
