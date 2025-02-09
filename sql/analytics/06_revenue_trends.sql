-- ============================================================================
-- Revenue Trends and Growth Analysis
-- ============================================================================
-- Purpose: Analyze revenue trends over time with period-over-period comparisons
--
-- Business Questions Answered:
-- - How is revenue trending month-over-month (MoM)?
-- - What is our year-over-year (YoY) growth rate?
-- - Are there accelerating or decelerating growth trends?
-- - What is the smoothed trend (removing monthly volatility)?
--
-- Key Metrics:
-- - Monthly revenue, order count, unique customers
-- - Month-over-month growth (absolute and percentage)
-- - Year-over-year growth comparison
-- - 3-month moving average for trend smoothing
--
-- Window Functions Used:
-- - LAG(): Access previous row values for period comparisons
-- - AVG() OVER(): Calculate rolling averages
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- Aggregate sales to monthly level and calculate growth metrics
WITH monthly_revenue AS (
  SELECT
    d.year,
    d.month,
    d.month_name,
    DATE_TRUNC('MONTH', d.date) AS month_date,
    SUM(f.total_amount) AS revenue,
    COUNT(DISTINCT f.invoice_no) AS order_count,
    COUNT(DISTINCT f.customer_key) AS unique_customers
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  GROUP BY d.year, d.month, d.month_name, DATE_TRUNC('MONTH', d.date)
)
SELECT
  year,
  month,
  month_name,
  ROUND(revenue, 2) AS revenue,
  order_count,
  unique_customers,
  ROUND(revenue / NULLIF(order_count, 0), 2) AS avg_order_value,

  -- Month-over-month growth
  -- LAG() retrieves the previous month's revenue for comparison
  ROUND(revenue - LAG(revenue) OVER (ORDER BY year, month), 2) AS mom_revenue_change,
  ROUND(
    (revenue - LAG(revenue) OVER (ORDER BY year, month)) * 100.0 /
    NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0),
    2
  ) AS mom_growth_pct,

  -- Year-over-year comparison
  -- LAG(revenue, 12) goes back 12 months to compare same month last year
  LAG(revenue, 12) OVER (ORDER BY year, month) AS revenue_same_month_last_year,
  ROUND(
    (revenue - LAG(revenue, 12) OVER (ORDER BY year, month)) * 100.0 /
    NULLIF(LAG(revenue, 12) OVER (ORDER BY year, month), 0),
    2
  ) AS yoy_growth_pct,

  -- 3-month moving average
  -- Smooths out short-term fluctuations to reveal underlying trends
  -- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW creates a 3-month window
  ROUND(AVG(revenue) OVER (
    ORDER BY year, month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS moving_avg_3month

FROM monthly_revenue
ORDER BY year, month;

-- ============================================================================
-- Business Insights from This Analysis:
-- ============================================================================
-- 1. MoM Growth: Positive values indicate growth, negative indicate decline
-- 2. YoY Growth: Compares to same period last year (accounts for seasonality)
-- 3. Moving Average: If actual revenue consistently above/below MA, indicates trend
-- 4. Use Cases:
--    - Financial forecasting and budget planning
--    - Identifying growth acceleration/deceleration
--    - Detecting anomalous months (outliers from MA)
--    - Board reporting and investor updates
-- ============================================================================
