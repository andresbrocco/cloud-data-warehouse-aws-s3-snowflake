-- ============================================================================
-- Customer Churn Analysis
-- ============================================================================
-- Purpose: Identify customers at risk of churning and categorize by lifecycle stage
--
-- Business Questions Answered:
-- - How many customers are active vs inactive?
-- - What percentage of customers are at risk of churning?
-- - How long has it been since churned customers last purchased?
-- - What is our current churn rate?
--
-- Key Concepts:
-- - Churn: When a customer stops purchasing from the business
-- - Churn Threshold: Time period after which we consider a customer churned
-- - Customer Lifecycle: Active → At Risk → Churning → Churned
--
-- Business Definitions (customizable based on business model):
-- - Active: Purchased within last 90 days
-- - At Risk: 91-180 days since last purchase (needs re-engagement)
-- - Churning: 181-365 days since last purchase (likely to churn soon)
-- - Churned: 365+ days since last purchase (assumed lost)
--
-- Note: These thresholds are arbitrary and should be calibrated to your
-- business's purchase cycle. High-frequency products (coffee) use shorter
-- windows; low-frequency (furniture) use longer windows.
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Customer Lifecycle Status and Churn Categorization
-- ============================================================================

WITH customer_last_order AS (
  -- Find the most recent order date for each customer
  SELECT
    c.customer_key,
    c.customer_id,
    MAX(d.date) AS last_order_date,
    -- Calculate days of inactivity (days since last purchase)
    DATEDIFF(DAY, MAX(d.date), CURRENT_DATE()) AS days_since_last_order
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_customer c ON f.customer_key = c.customer_key
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  GROUP BY c.customer_key, c.customer_id
)
SELECT
  -- Categorize customers into lifecycle stages based on recency
  CASE
    WHEN days_since_last_order <= 90 THEN 'Active (0-90 days)'
    WHEN days_since_last_order <= 180 THEN 'At Risk (91-180 days)'
    WHEN days_since_last_order <= 365 THEN 'Churning (181-365 days)'
    ELSE 'Churned (365+ days)'
  END AS customer_status,
  COUNT(*) AS customer_count,
  -- Calculate percentage of total customer base in each stage
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
  -- Average inactivity period for this lifecycle stage
  ROUND(AVG(days_since_last_order), 0) AS avg_days_since_last_order
FROM customer_last_order
GROUP BY
  CASE
    WHEN days_since_last_order <= 90 THEN 'Active (0-90 days)'
    WHEN days_since_last_order <= 180 THEN 'At Risk (91-180 days)'
    WHEN days_since_last_order <= 365 THEN 'Churning (181-365 days)'
    ELSE 'Churned (365+ days)'
  END
ORDER BY
  -- Sort by lifecycle stage progression
  CASE
    WHEN customer_status = 'Active (0-90 days)' THEN 1
    WHEN customer_status = 'At Risk (91-180 days)' THEN 2
    WHEN customer_status = 'Churning (181-365 days)' THEN 3
    ELSE 4
  END;

-- ============================================================================
-- How to Interpret Churn Analysis Results:
-- ============================================================================
-- 1. Healthy Distribution:
--    - Active: 40-60% (majority of customers recently engaged)
--    - At Risk: 15-25% (moderate group needing re-engagement)
--    - Churning: 10-20% (small group, needs immediate intervention)
--    - Churned: 10-30% (expected attrition, unlikely to recover)
--
-- 2. Warning Signs:
--    - Active < 30%: Significant retention problem
--    - At Risk + Churning > 50%: Need aggressive re-engagement campaigns
--    - Churned > 40%: High churn rate, investigate root causes
--
-- 3. Business Actions by Segment:
--
--    Active Customers:
--    - Nurture with loyalty programs
--    - Upsell/cross-sell opportunities
--    - Request referrals and reviews
--
--    At Risk Customers:
--    - Email re-engagement campaigns
--    - Personalized discount offers
--    - Survey to understand why they stopped purchasing
--
--    Churning Customers:
--    - Urgent win-back campaigns
--    - Special promotions or incentives
--    - Phone outreach for high-value customers
--
--    Churned Customers:
--    - Minimal investment (low ROI)
--    - Occasional win-back attempts
--    - Analyze exit reasons to prevent future churn
--
-- 4. Churn Rate Calculation:
--    Churn Rate = (Churned Customers / Total Customers) * 100
--    Example: If 20% are churned, churn rate = 20%
--
-- 5. Use Cases:
--    - Customer success team prioritization (focus on "At Risk" segment)
--    - Marketing budget allocation (re-engagement vs acquisition)
--    - Forecasting revenue loss from churn
--    - Product improvement roadmap (address reasons for churn)
--    - Executive dashboards and KPI tracking
--
-- 6. Advanced Extensions:
--    - Predict churn probability using ML models
--    - Calculate churn by customer segment (high-value vs low-value)
--    - Time-to-churn analysis (how long until customers typically churn?)
--    - Churn reasons analysis (combine with customer feedback data)
-- ============================================================================
