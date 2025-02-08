-- ============================================================================
-- RFM Analysis (Recency, Frequency, Monetary)
-- ============================================================================
-- Purpose: Segment customers based on their purchase behavior using the RFM
--          framework to identify customer engagement levels and target
--          marketing strategies effectively.
--
-- Business Questions:
--   - Which customers are our champions (recently purchased, frequently, high spend)?
--   - Who are at-risk customers that need re-engagement?
--   - How should we prioritize marketing efforts across customer segments?
--   - What is the distribution of customer value across our base?
--
-- RFM Framework:
--   - Recency: How recently did the customer make a purchase?
--   - Frequency: How often does the customer purchase?
--   - Monetary: How much does the customer spend?
--
-- SQL Techniques:
--   - Multi-level CTEs for step-by-step analysis
--   - Window functions (NTILE) for quintile scoring
--   - Date calculations (DATEDIFF) for recency
--   - CASE expressions for segment classification
--   - Aggregate functions for customer metrics
--
-- ============================================================================

USE SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Step 1: Calculate raw RFM metrics for each customer
WITH customer_rfm AS (
  SELECT
    c.customer_key,
    c.customer_id,

    -- Recency: days since last purchase (lower is better)
    DATEDIFF(DAY, MAX(d.date), CURRENT_DATE()) AS recency_days,

    -- Frequency: number of orders (higher is better)
    COUNT(DISTINCT f.invoice_no) AS frequency,

    -- Monetary: total revenue (higher is better)
    SUM(f.total_amount) AS monetary_value

  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_customer c ON f.customer_key = c.customer_key
  JOIN ECOMMERCE_DW.PRODUCTION.dim_date d ON f.date_key = d.date_key
  GROUP BY c.customer_key, c.customer_id
),

-- Step 2: Score each customer on a 1-5 scale for each RFM dimension
rfm_scores AS (
  SELECT
    customer_id,
    recency_days,
    frequency,
    ROUND(monetary_value, 2) AS monetary_value,

    -- Score 1-5 for each dimension (5 is best)
    -- Recency: Use DESC because lower days is better (more recent)
    NTILE(5) OVER (ORDER BY recency_days DESC) AS recency_score,

    -- Frequency: Use ASC because higher count is better
    NTILE(5) OVER (ORDER BY frequency ASC) AS frequency_score,

    -- Monetary: Use ASC because higher value is better
    NTILE(5) OVER (ORDER BY monetary_value ASC) AS monetary_score

  FROM customer_rfm
)

-- Step 3: Classify customers into actionable segments based on RFM scores
SELECT
  customer_id,
  recency_days,
  frequency,
  monetary_value,
  recency_score,
  frequency_score,
  monetary_score,
  (recency_score + frequency_score + monetary_score) AS rfm_total_score,

  -- Customer segmentation logic
  CASE
    -- Champions: Recent, frequent, high-value customers (score 12+)
    WHEN (recency_score + frequency_score + monetary_score) >= 12 THEN 'Champions'

    -- Loyal Customers: Frequent buyers with good recency (score 9-11)
    WHEN (recency_score + frequency_score + monetary_score) >= 9 THEN 'Loyal Customers'

    -- Potential Loyalists: Recent customers with growth potential (score 6-8)
    WHEN (recency_score + frequency_score + monetary_score) >= 6 THEN 'Potential Loyalists'

    -- At Risk: Previously engaged but low recent activity
    WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'At Risk'

    -- Hibernating: Haven't purchased recently
    WHEN recency_score <= 2 THEN 'Hibernating'

    -- Need Attention: All other customers requiring engagement
    ELSE 'Need Attention'
  END AS customer_segment

FROM rfm_scores
ORDER BY rfm_total_score DESC, monetary_value DESC
LIMIT 100;

-- ============================================================================
-- Expected Output Columns:
--   - customer_id: Unique customer identifier
--   - recency_days: Days since last purchase
--   - frequency: Total number of orders
--   - monetary_value: Total lifetime spend
--   - recency_score: Recency quintile (1-5, 5 is best)
--   - frequency_score: Frequency quintile (1-5, 5 is best)
--   - monetary_score: Monetary quintile (1-5, 5 is best)
--   - rfm_total_score: Sum of all three scores (3-15)
--   - customer_segment: Business-friendly segment label
--
-- Customer Segments Explained:
--   - Champions: Best customers - nurture and reward
--   - Loyal Customers: Regular buyers - upsell opportunities
--   - Potential Loyalists: Growing customers - build relationship
--   - At Risk: Declining engagement - win-back campaigns
--   - Hibernating: Inactive - reactivation campaigns
--   - Need Attention: Mixed signals - personalized engagement
--
-- Business Actions:
--   - Champions: VIP programs, early access, referral incentives
--   - Loyal Customers: Cross-sell, bundle offers
--   - Potential Loyalists: Loyalty programs, personalized recommendations
--   - At Risk: Special discounts, feedback surveys
--   - Hibernating: Win-back emails, aggressive promotions
--   - Need Attention: Re-engagement campaigns, A/B testing
-- ============================================================================
