-- ============================================================================
-- Customer Segmentation Analysis
-- ============================================================================
-- Purpose: Segment customers into value tiers (High/Medium/Low) based on total
--          spend using statistical methods (percentiles), then analyze segment
--          characteristics to inform targeted marketing and service strategies.
--
-- Business Questions:
--   - How should we define customer value tiers?
--   - What are the behavioral characteristics of each segment?
--   - Which countries have the most high-value customers?
--   - What is the size and potential of each customer segment?
--
-- SQL Techniques:
--   - PERCENTILE_CONT for statistical distribution analysis
--   - Subqueries in CASE expressions for dynamic thresholds
--   - WITHIN GROUP clause for ordered set functions
--   - Multi-level aggregations (customer-level then segment-level)
--   - CTEs for query organization
--   - Custom sorting with CASE expressions in ORDER BY
--
-- ============================================================================

USE SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Step 1: Calculate comprehensive customer summary metrics
WITH customer_summary AS (
  SELECT
    c.customer_key,
    c.customer_id,
    co.country_name,

    -- Order behavior metrics
    COUNT(DISTINCT f.invoice_no) AS order_count,
    SUM(f.total_amount) AS total_spend,
    AVG(f.total_amount) AS avg_order_value,

    -- Product diversity metric
    COUNT(DISTINCT p.product_key) AS unique_products_purchased

  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_customer c ON f.customer_key = c.customer_key
  JOIN ECOMMERCE_DW.PRODUCTION.dim_country co ON c.country_key = co.country_key
  JOIN ECOMMERCE_DW.PRODUCTION.dim_product p ON f.product_key = p.product_key
  GROUP BY c.customer_key, c.customer_id, co.country_name
)

-- Step 2: Classify customers into value segments using percentiles
-- and aggregate segment characteristics
SELECT
  -- Define customer value segments using statistical percentiles
  CASE
    -- High Value: Top 10% of customers by spend (90th percentile+)
    WHEN total_spend >= (SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_spend) FROM customer_summary)
      THEN 'High Value'

    -- Medium Value: 60th-90th percentile (middle-upper tier)
    WHEN total_spend >= (SELECT PERCENTILE_CONT(0.60) WITHIN GROUP (ORDER BY total_spend) FROM customer_summary)
      THEN 'Medium Value'

    -- Low Value: Bottom 60% (but still customers)
    ELSE 'Low Value'
  END AS customer_value_segment,

  country_name,

  -- Segment size and characteristics
  COUNT(*) AS customer_count,
  ROUND(AVG(total_spend), 2) AS avg_total_spend,
  ROUND(AVG(order_count), 1) AS avg_orders_per_customer,
  ROUND(AVG(avg_order_value), 2) AS avg_order_value,
  ROUND(AVG(unique_products_purchased), 1) AS avg_unique_products

FROM customer_summary

GROUP BY customer_value_segment, country_name

-- Order by segment tier (High > Medium > Low) then by customer count
ORDER BY
  CASE customer_value_segment
    WHEN 'High Value' THEN 1
    WHEN 'Medium Value' THEN 2
    ELSE 3
  END,
  customer_count DESC;

-- ============================================================================
-- Expected Output Columns:
--   - customer_value_segment: Customer tier (High/Medium/Low Value)
--   - country_name: Country of customer group
--   - customer_count: Number of customers in this segment/country
--   - avg_total_spend: Average lifetime spend per customer in segment
--   - avg_orders_per_customer: Average number of orders in segment
--   - avg_order_value: Average spend per order in segment
--   - avg_unique_products: Average product variety purchased
--
-- Segmentation Logic Explained:
--
--   PERCENTILE_CONT (continuous percentile):
--   - 90th percentile: Top 10% of customers (High Value)
--   - 60th percentile: Top 40% of customers (Medium Value cutoff)
--   - Bottom 60%: Low Value
--
--   This creates a pyramid structure:
--   - High Value: ~10% of customers (should generate ~40-50% of revenue)
--   - Medium Value: ~30% of customers (generates ~30-35% of revenue)
--   - Low Value: ~60% of customers (generates ~15-30% of revenue)
--
--   Percentile-based segmentation is dynamic - thresholds adjust as customer
--   base evolves, unlike fixed dollar amount segments.
--
-- Business Insights:
--   - Customer distribution across value tiers
--   - Geographic concentration of high-value customers
--   - Behavioral differences between segments (order frequency, AOV, etc.)
--   - Revenue concentration analysis (Pareto principle validation)
--
-- Business Actions:
--
--   High Value Customers:
--   - Assign dedicated account managers
--   - VIP customer service (priority support, dedicated hotline)
--   - Exclusive early access to new products
--   - Personalized communication and offers
--   - Loyalty rewards and recognition programs
--
--   Medium Value Customers:
--   - Upsell and cross-sell campaigns
--   - Loyalty program incentives to move to High tier
--   - Bundle offers to increase avg_order_value
--   - Encourage product variety exploration
--
--   Low Value Customers:
--   - Automated marketing campaigns
--   - Volume-based promotions to increase order frequency
--   - First-purchase discounts for new customers
--   - Cost-efficient service channels (self-service, chatbots)
--
-- Example Interpretation:
--   If UK has 100 High Value customers with avg_total_spend of $15,000
--   and avg_orders_per_customer of 45, this indicates a loyal, high-frequency
--   segment worth significant retention investment.
-- ============================================================================
