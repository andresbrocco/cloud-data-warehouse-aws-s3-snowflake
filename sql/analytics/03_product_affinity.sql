-- ============================================================================
-- Product Affinity Analysis: Products Frequently Bought Together
-- ============================================================================
-- Purpose: Identify product pairs that are frequently purchased together in
--          the same order to enable cross-selling, bundling strategies, and
--          merchandising optimization (market basket analysis).
--
-- Business Questions:
--   - Which products are commonly purchased together?
--   - What bundling opportunities exist in our catalog?
--   - How can we optimize product placement and recommendations?
--   - What percentage of orders include these product combinations?
--
-- SQL Techniques:
--   - Self-joins to find product pairs within same invoice
--   - CTEs for query organization
--   - Inequality joins (product_key < product_key) to avoid duplicates
--   - Aggregate functions (COUNT) for co-purchase metrics
--   - HAVING clause for filtering significant relationships
--   - Subquery in SELECT for percentage calculations
--
-- ============================================================================

USE SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Step 1: Create a list of distinct products per order
WITH order_products AS (
  SELECT DISTINCT
    f.invoice_no,
    p.product_key,
    p.stock_code,
    p.description
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
  JOIN ECOMMERCE_DW.PRODUCTION.dim_product p ON f.product_key = p.product_key
)

-- Step 2: Self-join to find products purchased in the same order
SELECT
  p1.stock_code AS product_a_code,
  p1.description AS product_a_desc,
  p2.stock_code AS product_b_code,
  p2.description AS product_b_desc,

  -- Count how many times this pair was purchased together
  COUNT(*) AS times_purchased_together,

  -- Calculate what percentage of all orders include this product pair
  ROUND(COUNT(*) * 100.0 / (
    SELECT COUNT(DISTINCT invoice_no)
    FROM ECOMMERCE_DW.PRODUCTION.fact_sales
  ), 2) AS co_purchase_percentage

FROM order_products p1
JOIN order_products p2
  ON p1.invoice_no = p2.invoice_no  -- Same order
  AND p1.product_key < p2.product_key  -- Avoid duplicates (A,B) vs (B,A) and self-joins (A,A)

GROUP BY
  p1.stock_code, p1.description,
  p2.stock_code, p2.description

-- Filter for statistically significant relationships
HAVING COUNT(*) >= 10  -- At least 10 co-purchases to be meaningful

ORDER BY times_purchased_together DESC
LIMIT 50;

-- ============================================================================
-- Expected Output Columns:
--   - product_a_code: Stock code of first product in pair
--   - product_a_desc: Description of first product
--   - product_b_code: Stock code of second product in pair
--   - product_b_desc: Description of second product
--   - times_purchased_together: Number of orders containing both products
--   - co_purchase_percentage: Percentage of all orders with this combination
--
-- Query Logic Explained:
--   - Self-join finds all product pairs within the same invoice
--   - p1.product_key < p2.product_key ensures each pair appears once
--     (prevents both (A,B) and (B,A), and prevents (A,A))
--   - HAVING COUNT(*) >= 10 filters out random noise (requires significance)
--
-- Business Insights:
--   - Top 50 product combinations by co-purchase frequency
--   - Strength of product relationships (co-purchase percentage)
--   - Opportunities for product bundling and promotions
--
-- Business Actions:
--   - Create product bundles for frequently paired items
--   - Place complementary products near each other in stores/website
--   - Implement "customers who bought X also bought Y" recommendations
--   - Design promotional campaigns around popular product combinations
--   - Optimize inventory management for correlated products
--
-- Example Interpretation:
--   If "White Hanging Heart T-Light Holder" and "Cream Cupid Hearts Coat Hanger"
--   appear together in 50 orders (1.2% of all orders), this suggests a strong
--   home decor theme affinity that could be leveraged for merchandising.
-- ============================================================================
