-- ============================================================================
-- Materialized Views for Performance Optimization
-- ============================================================================
-- Purpose: Pre-compute expensive aggregations for faster query performance
--
-- What are Materialized Views?
-- - Physical storage of query results (unlike regular views which are virtual)
-- - Automatically refresh when underlying tables change (within minutes)
-- - Trade storage cost for query speed on frequently-run aggregations
--
-- When to Use Materialized Views:
-- - Dashboard queries that run many times per day
-- - Complex aggregations over large fact tables
-- - Queries with consistent GROUP BY patterns
-- - Reports requiring sub-second response times
--
-- Cost Considerations:
-- - Storage: Stores pre-computed results (additional GB charged)
-- - Compute: Background refresh operations when source data changes
-- - Trade-off: Spend on storage/refresh to save on repeated query compute
--
-- Note: For this portfolio project with ~500K rows, gains are modest.
--       In production with billions of rows, MV speedups can be 10-100x.
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Materialized View 1: Customer Summary
-- ============================================================================
-- Purpose: Pre-aggregate customer lifetime metrics for fast lookups
-- Use Cases:
--   - Customer 360 dashboards
--   - Segmentation analysis (high-value customers)
--   - Churn risk scoring (days since last order)
--   - Account health monitoring
--
-- Refresh Trigger: Changes to fact_sales, dim_customer, or dim_country
-- Query Speedup: Eliminates ~400K row scan and aggregation on every query
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW mv_customer_summary AS
SELECT
  c.customer_key,
  c.customer_id,
  co.country_name,
  c.first_order_date,
  c.last_order_date,
  COUNT(DISTINCT f.invoice_no) AS total_orders,
  SUM(f.quantity) AS total_items_purchased,
  SUM(f.total_amount) AS lifetime_revenue,
  AVG(f.total_amount) AS avg_order_value,
  MAX(d.date) AS most_recent_order_date,
  DATEDIFF(DAY, MAX(d.date), CURRENT_DATE()) AS days_since_last_order
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_country co ON c.country_key = co.country_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
  c.customer_key,
  c.customer_id,
  co.country_name,
  c.first_order_date,
  c.last_order_date;

-- ============================================================================
-- Materialized View 2: Product Summary
-- ============================================================================
-- Purpose: Pre-aggregate product performance metrics for fast analysis
-- Use Cases:
--   - Product performance dashboards
--   - Inventory planning (fast vs slow movers)
--   - Pricing optimization (compare current vs avg selling price)
--   - Product lifecycle analysis (first/last sale dates)
--
-- Refresh Trigger: Changes to fact_sales, dim_product, or dim_category
-- Query Speedup: Eliminates product-level aggregation across all transactions
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW mv_product_summary AS
SELECT
  p.product_key,
  p.stock_code,
  p.description,
  cat.category_name,
  COUNT(DISTINCT f.invoice_no) AS times_ordered,
  SUM(f.quantity) AS total_quantity_sold,
  SUM(f.total_amount) AS total_revenue,
  AVG(f.unit_price) AS avg_selling_price,
  COUNT(DISTINCT f.customer_key) AS unique_customers,
  MIN(d.date) AS first_sale_date,
  MAX(d.date) AS last_sale_date
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_category cat ON p.category_key = cat.category_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
  p.product_key,
  p.stock_code,
  p.description,
  cat.category_name;

-- ============================================================================
-- Materialized View 3: Daily Sales Summary
-- ============================================================================
-- Purpose: Pre-aggregate daily metrics for time-series analysis
-- Use Cases:
--   - Revenue trend charts (daily/monthly/yearly)
--   - Seasonality analysis (weekend vs weekday)
--   - Business health monitoring (orders per day, AOV)
--   - Forecasting (historical patterns for predictive models)
--
-- Refresh Trigger: Changes to fact_sales or dim_date
-- Query Speedup: Eliminates daily aggregation, enables fast drill-down
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW mv_daily_sales AS
SELECT
  d.date_key,
  d.date,
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  d.day_name,
  d.is_weekend,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  COUNT(DISTINCT f.customer_key) AS unique_customers,
  SUM(f.quantity) AS total_items_sold,
  SUM(f.total_amount) AS daily_revenue,
  AVG(f.total_amount) AS avg_transaction_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
  d.date_key,
  d.date,
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  d.day_name,
  d.is_weekend;

-- ============================================================================
-- Verify Materialized Views
-- ============================================================================
-- This command shows all materialized views with metadata:
-- - Name, database, schema
-- - Owner, comment
-- - Creation timestamp
-- - Last refresh time (important for monitoring staleness)
-- ============================================================================

SHOW MATERIALIZED VIEWS IN SCHEMA ECOMMERCE_DW.PRODUCTION;

-- ============================================================================
-- Usage Notes
-- ============================================================================
-- 1. Refresh Behavior:
--    - Automatic background refresh (usually within minutes of source change)
--    - No need to manually refresh in most cases
--    - Check last_altered_time in SHOW MATERIALIZED VIEWS output
--
-- 2. Query Pattern:
--    - Use MV just like a regular table: SELECT * FROM mv_customer_summary
--    - Add WHERE clauses for filtering (still benefits from pre-aggregation)
--    - Join with other tables if needed
--
-- 3. Monitoring:
--    - Query QUERY_HISTORY to compare performance before/after MV
--    - Check bytes_scanned (should decrease significantly)
--    - Verify execution_time improvements
--
-- 4. Maintenance:
--    - Snowflake handles refresh automatically
--    - If base table schema changes, may need to recreate MV
--    - Use CREATE OR REPLACE to update MV definition
--
-- 5. Alternatives:
--    - For very infrequent queries: use regular views (no storage cost)
--    - For static snapshots: use tables with manual refresh ETL
--    - For real-time: query base tables directly (no MV staleness)
-- ============================================================================
