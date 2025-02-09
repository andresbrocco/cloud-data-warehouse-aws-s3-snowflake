-- ============================================================================
-- Seasonality and Pattern Analysis
-- ============================================================================
-- Purpose: Identify seasonal patterns and cyclical trends in shopping behavior
--
-- Business Questions Answered:
-- - Which months generate the highest revenue (holiday season effect)?
-- - What days of the week are most popular for shopping?
-- - Is there a significant difference between weekend and weekday behavior?
-- - How concentrated is revenue across the calendar?
--
-- Key Metrics:
-- - Monthly revenue distribution and rankings
-- - Day of week performance metrics
-- - Weekend vs weekday comparison
--
-- Business Applications:
-- - Inventory planning for peak seasons
-- - Marketing campaign timing
-- - Staffing and resource allocation
-- - Cash flow forecasting
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Part 1: Monthly Seasonality Analysis
-- ============================================================================
-- Identifies which months consistently drive the most revenue
-- Expected pattern: Q4 (Oct-Dec) should be highest for retail (holiday shopping)

WITH monthly_stats AS (
  SELECT
    d.month,
    d.month_name,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.invoice_no) AS total_orders,
    AVG(f.total_amount) AS avg_transaction_value
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  GROUP BY d.month, d.month_name
)
SELECT
  month_name,
  ROUND(total_revenue, 2) AS total_revenue,
  total_orders,
  ROUND(avg_transaction_value, 2) AS avg_transaction_value,
  -- Calculate percentage contribution of each month to annual revenue
  ROUND(
    total_revenue * 100.0 / SUM(total_revenue) OVER (),
    2
  ) AS revenue_percentage,
  RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM monthly_stats
ORDER BY month;

-- ============================================================================
-- Part 2: Day of Week Analysis
-- ============================================================================
-- Reveals weekly shopping patterns and preferred shopping days

SELECT
  d.day_name,
  d.day_of_week,
  d.is_weekend,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  SUM(f.total_amount) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_order_value,
  COUNT(DISTINCT f.customer_key) AS unique_customers
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
GROUP BY d.day_name, d.day_of_week, d.is_weekend
ORDER BY d.day_of_week;

-- ============================================================================
-- Part 3: Weekend vs Weekday Comparison
-- ============================================================================
-- Tests hypothesis: Do customers shop differently on weekends vs weekdays?
-- Important for marketing scheduling and promotional timing

SELECT
  CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  COUNT(DISTINCT d.date) AS days_count,
  SUM(f.total_amount) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_daily_revenue,
  COUNT(DISTINCT f.invoice_no) AS total_orders,
  -- Calculate orders per day to normalize for different number of weekdays vs weekend days
  ROUND(COUNT(DISTINCT f.invoice_no) * 1.0 / COUNT(DISTINCT d.date), 2) AS avg_orders_per_day
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
GROUP BY CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END;

-- ============================================================================
-- Business Insights from This Analysis:
-- ============================================================================
-- 1. Monthly Seasonality:
--    - Identifies peak shopping months (likely Nov-Dec for retail)
--    - Informs inventory purchasing and warehouse capacity planning
--    - Guides annual marketing budget allocation
--
-- 2. Day of Week Patterns:
--    - Reveals preferred shopping days
--    - Helps schedule promotions and flash sales
--    - Informs customer service staffing levels
--
-- 3. Weekend vs Weekday:
--    - Tests behavioral differences between work days and leisure days
--    - May inform email campaign send times
--    - Could reveal different customer segments (B2B vs B2C behavior)
--
-- 4. Use Cases:
--    - Supply chain optimization (stock up before peak months)
--    - Marketing calendar planning
--    - Workforce scheduling
--    - Cash flow forecasting (expect higher revenue in certain periods)
-- ============================================================================
