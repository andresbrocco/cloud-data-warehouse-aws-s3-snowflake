-- ============================================================================
-- Product Performance Analysis with Window Functions
-- ============================================================================
-- Purpose: Analyze product sales trends over time using advanced window
--          functions to identify top performers, track momentum, and
--          calculate moving averages for demand forecasting.
--
-- Business Questions:
--   - Which products are top sellers each month?
--   - How is product revenue trending over time?
--   - What is the sales momentum (3-month moving average)?
--   - Which products show consistent growth vs. volatile performance?
--
-- SQL Techniques:
--   - Window functions (RANK, SUM, AVG) with PARTITION BY and ORDER BY
--   - Frame clauses (ROWS BETWEEN) for running totals and moving averages
--   - CTEs for query organization
--   - Subquery in WHERE clause for dynamic filtering
--   - Multi-column aggregations and grouping
--
-- ============================================================================

USE SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Step 1: Aggregate product sales by month
WITH product_monthly_sales AS (
  SELECT
    d.year,
    d.month,
    d.month_name,
    p.stock_code,
    p.description,
    SUM(f.quantity) AS units_sold,
    SUM(f.total_amount) AS revenue
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  JOIN ECOMMERCE_DW.PRODUCTION.dim_product p ON f.product_key = p.product_key
  GROUP BY d.year, d.month, d.month_name, p.stock_code, p.description
)

-- Step 2: Apply window functions for advanced analytics
SELECT
  year,
  month,
  month_name,
  stock_code,
  description,
  units_sold,
  ROUND(revenue, 2) AS revenue,

  -- Rank products within each month by revenue
  -- Shows which products are top performers in each time period
  RANK() OVER (
    PARTITION BY year, month
    ORDER BY revenue DESC
  ) AS monthly_rank,

  -- Running total of revenue for each product over time
  -- Shows cumulative revenue growth trajectory
  ROUND(SUM(revenue) OVER (
    PARTITION BY stock_code
    ORDER BY year, month
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ), 2) AS cumulative_revenue,

  -- 3-month moving average to smooth out volatility
  -- Shows underlying trend excluding seasonal spikes
  ROUND(AVG(revenue) OVER (
    PARTITION BY stock_code
    ORDER BY year, month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS moving_avg_3month

FROM product_monthly_sales

-- Focus on top 5 products by total revenue for readability
WHERE stock_code IN (
  SELECT stock_code
  FROM product_monthly_sales
  GROUP BY stock_code
  ORDER BY SUM(revenue) DESC
  LIMIT 5
)

ORDER BY stock_code, year, month;

-- ============================================================================
-- Expected Output Columns:
--   - year: Year of sale
--   - month: Month number (1-12)
--   - month_name: Month name (January, February, etc.)
--   - stock_code: Product stock code
--   - description: Product description
--   - units_sold: Quantity sold in this month
--   - revenue: Total revenue for this month
--   - monthly_rank: Product's rank within this month (1 = top seller)
--   - cumulative_revenue: Running total of revenue up to this month
--   - moving_avg_3month: 3-month moving average revenue
--
-- Window Function Explanations:
--
--   1. RANK() with PARTITION BY:
--      - Resets ranking for each month
--      - Allows comparison of product performance within time periods
--      - Identifies which products are consistently top-ranked
--
--   2. SUM() with UNBOUNDED PRECEDING:
--      - Creates running total from first row to current row
--      - Shows cumulative revenue trajectory
--      - Useful for identifying inflection points in product lifecycle
--
--   3. AVG() with 2 PRECEDING:
--      - Calculates average of current row and 2 prior rows
--      - Smooths out month-to-month volatility
--      - First month shows actual value, second month shows 2-month avg,
--        third month onwards shows true 3-month moving average
--
-- Business Insights:
--   - Top 5 products by lifetime revenue with monthly breakdown
--   - Product ranking stability (consistent top-rankers vs. volatile)
--   - Revenue growth patterns (steady, accelerating, declining)
--   - Seasonal patterns visible in moving average vs. actual revenue
--
-- Business Actions:
--   - Inventory planning based on moving averages (demand forecasting)
--   - Identify products losing momentum (declining cumulative slope)
--   - Allocate marketing budget to products with improving trends
--   - Plan promotions around products with volatile seasonal patterns
--   - Investigate top-ranked products for expansion opportunities
--
-- Example Interpretation:
--   If a product shows monthly_rank=1 consistently but moving_avg_3month
--   is declining, it suggests the product is still popular but losing
--   momentum - time for a refresh or promotion.
-- ============================================================================
