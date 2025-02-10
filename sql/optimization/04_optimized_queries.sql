-- ============================================================================
-- Optimized Queries Using Materialized Views
-- ============================================================================
-- Purpose: Demonstrate performance benefits of materialized views
--
-- Before Optimization:
-- - Queries scan ~400K fact_sales rows and perform expensive aggregations
-- - Execution time: seconds (varies by complexity and warehouse size)
-- - Bytes scanned: 10s-100s of MB depending on query
--
-- After Optimization (using MVs):
-- - Queries scan pre-aggregated MV (much smaller row count)
-- - Execution time: milliseconds (up to 10-100x faster in production)
-- - Bytes scanned: MBs instead of GBs (huge savings on large tables)
--
-- Note: For this portfolio project (~500K rows), speedups are modest.
--       In production with billions of fact rows, MVs are transformational.
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Query 1: Top Customers by Lifetime Value (using MV)
-- ============================================================================
-- Business Use Case: Identify VIP customers for loyalty programs
--
-- Without MV: Scans fact_sales (~400K rows), aggregates by customer
-- With MV: Scans mv_customer_summary (~4K rows) - already aggregated
--
-- Performance Gain: 100x fewer rows scanned, no aggregation compute
-- ============================================================================

SELECT
  customer_id,
  country_name,
  total_orders,
  lifetime_revenue,
  avg_order_value,
  days_since_last_order
FROM mv_customer_summary
WHERE lifetime_revenue > 1000  -- High-value customers only
ORDER BY lifetime_revenue DESC
LIMIT 50;

-- ============================================================================
-- Query 2: Product Performance Analysis (using MV)
-- ============================================================================
-- Business Use Case: Identify best-selling products for inventory planning
--
-- Without MV: Scans fact_sales, joins dim_product, aggregates by product
-- With MV: Scans mv_product_summary (~4K rows) - all metrics pre-computed
--
-- Performance Gain: No joins, no aggregation, instant results
-- ============================================================================

SELECT
  stock_code,
  description,
  category_name,
  total_revenue,
  total_quantity_sold,
  unique_customers
FROM mv_product_summary
ORDER BY total_revenue DESC
LIMIT 20;

-- ============================================================================
-- Query 3: Monthly Revenue Trend (using MV)
-- ============================================================================
-- Business Use Case: Revenue trend dashboard for executive reporting
--
-- Without MV: Scans fact_sales, joins dim_date, aggregates by month
-- With MV: Scans mv_daily_sales (already daily aggregates), groups by month
--
-- Performance Gain: Much smaller dataset (days not transactions), faster aggregation
-- ============================================================================

SELECT
  year,
  month_name,
  SUM(daily_revenue) AS monthly_revenue,
  SUM(order_count) AS monthly_orders,
  AVG(daily_revenue) AS avg_daily_revenue
FROM mv_daily_sales
GROUP BY year, month, month_name
ORDER BY year, month;

-- ============================================================================
-- Query 4: Weekend vs Weekday Performance (using MV)
-- ============================================================================
-- Business Use Case: Staffing and marketing decisions based on day type
--
-- Without MV: Scans fact_sales, joins dim_date, classifies days, aggregates
-- With MV: Scans mv_daily_sales (is_weekend already computed), simple aggregation
--
-- Performance Gain: Pre-computed is_weekend flag, no date logic needed
-- ============================================================================

SELECT
  CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  COUNT(*) AS days_count,
  SUM(daily_revenue) AS total_revenue,
  AVG(daily_revenue) AS avg_daily_revenue,
  SUM(order_count) AS total_orders
FROM mv_daily_sales
GROUP BY CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END;

-- ============================================================================
-- Comparison: Query with and without Materialized View
-- ============================================================================
-- To measure actual performance gains, run both versions:
--
-- Version 1 (WITHOUT MV - direct from fact table):
-- SELECT
--   c.customer_id,
--   SUM(f.total_amount) AS lifetime_revenue
-- FROM fact_sales f
-- JOIN dim_customer c ON f.customer_key = c.customer_key
-- GROUP BY c.customer_id
-- ORDER BY lifetime_revenue DESC
-- LIMIT 50;
--
-- Version 2 (WITH MV - from pre-aggregated view):
-- SELECT customer_id, lifetime_revenue
-- FROM mv_customer_summary
-- ORDER BY lifetime_revenue DESC
-- LIMIT 50;
--
-- Compare:
-- - total_elapsed_time (execution time in milliseconds)
-- - bytes_scanned (data read from storage)
-- - rows_produced (should be same, but MV gets there faster)
--
-- Check query history:
-- SELECT query_id, query_text, total_elapsed_time, bytes_scanned
-- FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
-- WHERE query_text LIKE '%lifetime_revenue%'
-- ORDER BY start_time DESC LIMIT 5;
-- ============================================================================

-- ============================================================================
-- Best Practices
-- ============================================================================
-- 1. When to Query MVs:
--    - Dashboard queries (run frequently, need sub-second response)
--    - Report generation (consistent aggregations, user-facing)
--    - API endpoints (low latency requirements)
--
-- 2. When to Query Base Tables:
--    - Ad-hoc analysis (one-off queries, flexibility more important than speed)
--    - Real-time requirements (can't wait for MV refresh)
--    - Queries with filters MV doesn't support (complex WHERE clauses)
--
-- 3. Combining Optimizations:
--    - Use MVs for aggregation speedup
--    - Add clustering to MVs if they're queried with date filters
--    - Enable search optimization on MV for customer_id/product_key lookups
--
-- 4. Monitoring:
--    - Check MV refresh frequency (SHOW MATERIALIZED VIEWS)
--    - Verify queries are actually using MVs (query profile in UI)
--    - Compare costs: MV storage/refresh vs repeated query compute
-- ============================================================================
