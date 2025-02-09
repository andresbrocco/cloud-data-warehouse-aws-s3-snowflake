-- ============================================================================
-- Peak Shopping Times and Revenue Concentration Analysis
-- ============================================================================
-- Purpose: Identify the highest revenue days and analyze revenue distribution
--
-- Business Questions Answered:
-- - Which specific days generated the most revenue?
-- - What percentage of total revenue comes from the top 20% of days?
-- - Is revenue evenly distributed or highly concentrated?
-- - What characteristics do peak days share (day of week, month, year)?
--
-- Key Concepts:
-- - Pareto Principle (80/20 Rule): Often ~80% of revenue comes from ~20% of days
-- - Revenue Concentration: How concentrated revenue is across time periods
-- - Peak Period Identification: Finding outlier high-revenue days
--
-- Business Applications:
-- - Understand revenue volatility and predictability
-- - Identify promotional campaign success
-- - Plan inventory for expected peak periods
-- - Forecast revenue based on historical peak patterns
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Part 1: Top Revenue Days
-- ============================================================================
-- Identifies the highest-grossing individual days
-- Useful for understanding exceptional sales events and their characteristics

SELECT
  d.date,
  d.day_name,
  d.month_name,
  d.year,
  SUM(f.total_amount) AS daily_revenue,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  COUNT(DISTINCT f.customer_key) AS unique_customers,
  RANK() OVER (ORDER BY SUM(f.total_amount) DESC) AS revenue_rank
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
GROUP BY d.date, d.day_name, d.month_name, d.year
ORDER BY daily_revenue DESC
LIMIT 20;

-- ============================================================================
-- Part 2: Revenue Concentration Analysis (Pareto Analysis)
-- ============================================================================
-- Tests the 80/20 rule: Do 20% of days generate 80% of revenue?
-- Shows cumulative revenue contribution as we go down the ranked days

WITH daily_revenue AS (
  -- Calculate total revenue for each day
  SELECT
    d.date,
    SUM(f.total_amount) AS revenue
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  GROUP BY d.date
),
revenue_ranked AS (
  -- Rank days by revenue and calculate cumulative totals
  SELECT
    date,
    revenue,
    -- Running total of revenue as we go down the ranked list
    SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
    -- Total revenue across all days (for percentage calculations)
    SUM(revenue) OVER () AS total_revenue,
    -- Rank of this day (1 = highest revenue day)
    ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rank,
    -- Total number of days in dataset
    COUNT(*) OVER () AS total_days
  FROM daily_revenue
)
SELECT
  rank,
  date,
  ROUND(revenue, 2) AS daily_revenue,
  ROUND(cumulative_revenue, 2) AS cumulative_revenue,
  -- What percentage of total revenue has been accumulated so far?
  ROUND(cumulative_revenue * 100.0 / total_revenue, 2) AS cumulative_pct,
  -- What percentage of days have we covered so far?
  ROUND(rank * 100.0 / total_days, 2) AS days_pct
FROM revenue_ranked
WHERE rank <= 50  -- Top 50 days (adjust as needed)
ORDER BY rank;

-- ============================================================================
-- How to Interpret Peak Shopping Times Results:
-- ============================================================================
-- 1. Top Revenue Days:
--    - Look for patterns: Are they clustered in certain months?
--    - Check day of week: Are Fridays/Saturdays more common?
--    - Identify promotional events: Black Friday, Cyber Monday, etc.
--    - Outliers may indicate one-time events (viral marketing, PR coverage)
--
-- 2. Pareto Analysis Interpretation:
--    - Find where cumulative_pct reaches 80%
--    - Check corresponding days_pct at that point
--    - Example: If 80% revenue reached at 15% of days → highly concentrated
--    - Example: If 80% revenue reached at 60% of days → evenly distributed
--
-- 3. Revenue Concentration Patterns:
--    High Concentration (e.g., 20% days = 80% revenue):
--    - Indicates reliance on promotional events
--    - High revenue volatility
--    - Need to drive traffic on non-peak days
--
--    Low Concentration (e.g., 50% days = 80% revenue):
--    - More stable, predictable revenue
--    - Less dependent on promotions
--    - Indicates strong baseline demand
--
-- 4. Business Actions:
--    High-Concentration Scenarios:
--    - Replicate success of peak days (what made them work?)
--    - Diversify revenue streams to reduce volatility
--    - Plan cash flow around expected peaks
--
--    Low-Concentration Scenarios:
--    - Focus on consistent growth vs event-driven spikes
--    - Invest in customer retention (steady repeat purchases)
--
-- 5. Use Cases:
--    - Financial forecasting (understand revenue predictability)
--    - Marketing effectiveness (did campaign create peak day?)
--    - Inventory planning (prepare for expected peak periods)
--    - Performance benchmarking (compare current peaks to historical)
--    - Board reporting (demonstrate revenue concentration risk/stability)
--
-- 6. Advanced Extensions:
--    - Analyze peak days by customer segment (new vs repeat)
--    - Examine product mix on peak days (what drives the spikes?)
--    - Compare peak day characteristics year-over-year
--    - Model revenue distribution (fit statistical distribution)
--    - Forecast future peaks using time-series analysis
-- ============================================================================
