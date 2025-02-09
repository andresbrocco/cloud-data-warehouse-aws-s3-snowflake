-- ============================================================================
-- Cohort Retention Analysis
-- ============================================================================
-- Purpose: Track customer retention by acquisition cohort over time
--
-- Business Questions Answered:
-- - How many customers from each cohort return to purchase again?
-- - What is the retention rate after 1 month? 3 months? 12 months?
-- - Which acquisition cohorts have the best retention?
-- - Are retention rates improving or degrading over time?
--
-- Key Concepts:
-- - Cohort: Group of customers who made their first purchase in the same month
-- - Retention Rate: % of cohort members who return to purchase in subsequent months
-- - Months Since First Order: Time elapsed since customer's acquisition date
--
-- Business Applications:
-- - Product-market fit validation (good retention = strong PMF)
-- - Customer success team prioritization
-- - Lifetime value forecasting
-- - Marketing campaign effectiveness measurement
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Part 1: Detailed Cohort Retention Analysis
-- ============================================================================
-- Calculates retention rate for each cohort at each time period

WITH customer_cohorts AS (
  -- Assign each customer to their acquisition month cohort
  SELECT
    c.customer_key,
    c.customer_id,
    DATE_TRUNC('MONTH', c.first_order_date) AS cohort_month
  FROM ECOMMERCE_DW.PRODUCTION.dim_customer c
),
customer_orders AS (
  -- Get all months when each customer placed an order
  SELECT
    f.customer_key,
    DATE_TRUNC('MONTH', d.date) AS order_month
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  WHERE f.customer_key IS NOT NULL
  GROUP BY f.customer_key, DATE_TRUNC('MONTH', d.date)
),
cohort_data AS (
  -- Calculate how many months after acquisition each order occurred
  SELECT
    cc.cohort_month,
    co.order_month,
    DATEDIFF(MONTH, cc.cohort_month, co.order_month) AS months_since_first_order,
    COUNT(DISTINCT cc.customer_key) AS customer_count
  FROM customer_cohorts cc
  JOIN customer_orders co ON cc.customer_key = co.customer_key
  GROUP BY cc.cohort_month, co.order_month, DATEDIFF(MONTH, cc.cohort_month, co.order_month)
),
cohort_sizes AS (
  -- Get the initial size of each cohort (month 0)
  SELECT
    cohort_month,
    customer_count AS cohort_size
  FROM cohort_data
  WHERE months_since_first_order = 0
)
SELECT
  cd.cohort_month,
  cs.cohort_size,
  cd.months_since_first_order,
  cd.customer_count AS retained_customers,
  -- Retention rate = (customers who returned) / (initial cohort size) * 100
  ROUND(
    cd.customer_count * 100.0 / cs.cohort_size,
    2
  ) AS retention_rate_pct
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
WHERE cd.months_since_first_order <= 12  -- Show first 12 months of retention
ORDER BY cd.cohort_month, cd.months_since_first_order;

-- ============================================================================
-- Part 2: Simplified Cohort Visualization (Pivot-Like Format)
-- ============================================================================
-- Shows retention by cohort in a more readable, wide format
-- Each column represents a month since acquisition (0, 1, 2, 3, 6)

WITH customer_cohorts AS (
  SELECT
    c.customer_key,
    DATE_TRUNC('month', c.first_order_date)::DATE AS cohort_month
  FROM ECOMMERCE_DW.PRODUCTION.dim_customer c
),
customer_orders AS (
  SELECT
    f.customer_key,
    DATE_TRUNC('month', d.date)::DATE AS order_month
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  WHERE f.customer_key IS NOT NULL
  GROUP BY f.customer_key, DATE_TRUNC('month', d.date)
)
SELECT
  TO_CHAR(cc.cohort_month, 'YYYY-MM') AS cohort_month,
  COUNT(DISTINCT cc.customer_key) AS cohort_size,
  COUNT(DISTINCT CASE WHEN co.order_month = cc.cohort_month THEN cc.customer_key END) AS month_0,
  COUNT(DISTINCT CASE WHEN DATEDIFF('month', cc.cohort_month, co.order_month) = 1 THEN cc.customer_key END) AS month_1,
  COUNT(DISTINCT CASE WHEN DATEDIFF('month', cc.cohort_month, co.order_month) = 2 THEN cc.customer_key END) AS month_2,
  COUNT(DISTINCT CASE WHEN DATEDIFF('month', cc.cohort_month, co.order_month) = 3 THEN cc.customer_key END) AS month_3,
  COUNT(DISTINCT CASE WHEN DATEDIFF('month', cc.cohort_month, co.order_month) = 6 THEN cc.customer_key END) AS month_6
FROM customer_cohorts cc
LEFT JOIN customer_orders co ON cc.customer_key = co.customer_key
GROUP BY cc.cohort_month
ORDER BY cc.cohort_month;

-- ============================================================================
-- How to Interpret Cohort Retention Results:
-- ============================================================================
-- 1. Expected Pattern:
--    - Month 0: 100% (by definition, all customers in cohort made first purchase)
--    - Subsequent months: Declining retention (typical for most businesses)
--    - Example: 100% → 30% → 20% → 15% → 10%
--
-- 2. Good Retention vs Poor Retention:
--    - SaaS: >40% month 1, >20% month 6 is considered good
--    - E-commerce: >15% month 1, >5% month 6 is typical
--    - High retention = strong product-market fit
--
-- 3. Cohort Comparison:
--    - Are newer cohorts retaining better? → Product improvements working
--    - Are older cohorts retaining worse? → May indicate feature staleness
--
-- 4. Business Actions Based on Results:
--    - Low retention → Improve onboarding, product experience, customer support
--    - High early retention, low later → Implement loyalty programs
--    - Month 1 retention is critical → Focus on first-purchase experience
--
-- 5. Use Cases:
--    - Validate product changes (did retention improve after feature X?)
--    - Forecast LTV (use retention curve to predict future purchases)
--    - Identify customer success risks (cohorts with declining retention)
--    - Optimize marketing spend (focus on channels with high-retention cohorts)
-- ============================================================================
