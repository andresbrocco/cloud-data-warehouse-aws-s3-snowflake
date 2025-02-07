/*******************************************************************************
 * Script: 08_fact_validation_queries.sql
 * Purpose: Comprehensive validation and analysis queries for fact_sales table
 *
 * Description:
 *   This script provides a comprehensive set of validation and analytical queries
 *   to verify the integrity and business logic of the fact_sales table. It goes
 *   beyond basic data validation to demonstrate the analytical power of the
 *   dimensional model through business-focused queries.
 *
 *   Query Categories:
 *   1. Data Integrity Validation (row counts, totals, referential integrity)
 *   2. Dimensional Analysis (revenue by country, product, time)
 *   3. Business Metrics (top products, customer segments, trends)
 *   4. Data Quality Monitoring (NULL analysis, outliers)
 *
 * Prerequisites:
 *   1. fact_sales table populated (sql/production/07_load_fact_sales.sql)
 *   2. All dimension tables populated
 *
 * Execution Instructions:
 *   1. Execute queries individually or in groups to analyze data
 *   2. Review results to validate dimensional model
 *   3. Use query patterns for building production analytics
 *   4. Modify queries for specific business questions
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-07
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;
USE ROLE SYSADMIN;

/*******************************************************************************
 * SECTION 1: BASIC FACT TABLE STATISTICS
 * Purpose: High-level overview of fact table size and structure
 ******************************************************************************/

-- Total fact table row count
SELECT COUNT(*) AS total_fact_rows
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

-- Fact table storage statistics
SHOW TABLES LIKE 'fact_sales' IN SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Fact table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.fact_sales;

/*******************************************************************************
 * SECTION 2: GEOGRAPHIC ANALYSIS
 * Purpose: Revenue and order analysis by country and region
 ******************************************************************************/

-- ============================================================================
-- Total revenue by country (Top 10)
-- ============================================================================
-- Business Question: Which countries generate the most revenue?
-- Use Case: Market prioritization, resource allocation, expansion planning

SELECT
  c.country_name,
  COUNT(*) AS order_lines,
  SUM(f.quantity) AS total_units_sold,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value,
  COUNT(DISTINCT f.invoice_no) AS distinct_orders
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country c
  ON f.country_key = c.country_key
GROUP BY c.country_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Insights to look for:
-- - Is revenue concentrated in a few countries or distributed?
-- - Do high-revenue countries have high avg_line_item_value or high volume?
-- - Are there countries with low revenue but high potential?

-- ============================================================================
-- Revenue by region (Continental grouping)
-- ============================================================================
-- Business Question: How does revenue distribute across global regions?
-- Use Case: Regional strategy, logistics optimization, currency management

SELECT
  c.region,
  COUNT(DISTINCT c.country_name) AS countries_in_region,
  COUNT(*) AS order_lines,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country c
  ON f.country_key = c.country_key
GROUP BY c.region
ORDER BY total_revenue DESC;

-- Insights to look for:
-- - Which region dominates revenue? (Likely Europe for this dataset)
-- - Are there underperforming regions with growth potential?
-- - Does avg_line_item_value vary significantly by region?

/*******************************************************************************
 * SECTION 3: TEMPORAL ANALYSIS
 * Purpose: Revenue trends over time (monthly, quarterly, yearly)
 ******************************************************************************/

-- ============================================================================
-- Revenue by month (Time series)
-- ============================================================================
-- Business Question: What are monthly revenue trends? Are there seasonal patterns?
-- Use Case: Forecasting, inventory planning, marketing campaign timing

SELECT
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  ROUND(SUM(f.total_amount), 2) AS monthly_revenue,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  COUNT(*) AS line_item_count,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_date d
  ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.month;

-- Insights to look for:
-- - Is revenue growing, declining, or stable over time?
-- - Are there seasonal peaks (e.g., holiday shopping in November/December)?
-- - Are there unexpected dips that need investigation?

-- ============================================================================
-- Year-over-year revenue comparison
-- ============================================================================
-- Business Question: How does revenue compare year-over-year?
-- Use Case: Annual performance review, board reporting, growth tracking

SELECT
  d.year,
  ROUND(SUM(f.total_amount), 2) AS annual_revenue,
  COUNT(DISTINCT f.invoice_no) AS total_orders,
  COUNT(DISTINCT f.customer_key) AS active_customers,
  ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.invoice_no), 2) AS avg_order_value
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_date d
  ON f.date_key = d.date_key
GROUP BY d.year
ORDER BY d.year;

-- Insights to look for:
-- - What's the year-over-year growth rate?
-- - Is customer acquisition keeping pace with revenue growth?
-- - How is average order value trending?

-- ============================================================================
-- Weekend vs. weekday sales analysis
-- ============================================================================
-- Business Question: Does revenue differ between weekends and weekdays?
-- Use Case: Staffing optimization, campaign scheduling, customer behavior insights

SELECT
  d.is_weekend,
  CASE
    WHEN d.is_weekend THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  COUNT(*) AS order_lines,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value,
  COUNT(DISTINCT f.invoice_no) AS order_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_date d
  ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- Insights to look for:
-- - Are weekends higher or lower revenue than weekdays?
-- - Does customer behavior differ (avg_line_item_value, order_count)?
-- - Should marketing campaigns be timed differently?

/*******************************************************************************
 * SECTION 4: PRODUCT ANALYSIS
 * Purpose: Product performance, pricing, and category insights
 ******************************************************************************/

-- ============================================================================
-- Top 10 products by revenue
-- ============================================================================
-- Business Question: Which products generate the most revenue?
-- Use Case: Merchandising strategy, inventory prioritization, supplier negotiations

SELECT
  p.stock_code,
  p.description,
  cat.category_name,
  SUM(f.quantity) AS total_units_sold,
  ROUND(AVG(f.unit_price), 2) AS avg_selling_price,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  COUNT(DISTINCT f.invoice_no) AS orders_containing_product
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_product p
  ON f.product_key = p.product_key
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_category cat
  ON p.category_key = cat.category_key
WHERE p._is_current = TRUE
GROUP BY p.stock_code, p.description, cat.category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Insights to look for:
-- - Are top revenue products high-volume/low-price or low-volume/high-price?
-- - Do certain categories dominate the top 10?
-- - Are top products appearing in many orders (broad appeal)?

-- ============================================================================
-- Revenue by product category
-- ============================================================================
-- Business Question: How does revenue distribute across product categories?
-- Use Case: Category management, merchandising decisions, marketing focus

SELECT
  cat.category_name,
  COUNT(DISTINCT p.product_key) AS product_count,
  SUM(f.quantity) AS total_units_sold,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value,
  COUNT(DISTINCT f.invoice_no) AS orders_containing_category
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_product p
  ON f.product_key = p.product_key
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_category cat
  ON p.category_key = cat.category_key
WHERE p._is_current = TRUE
GROUP BY cat.category_name
ORDER BY total_revenue DESC;

-- Insights to look for:
-- - Which categories are revenue drivers vs. niche categories?
-- - Are there categories with high product count but low revenue (long tail)?
-- - Do some categories have significantly higher avg_line_item_value?

-- ============================================================================
-- Top 10 products by units sold (Volume leaders)
-- ============================================================================
-- Business Question: Which products sell the most units (regardless of price)?
-- Use Case: Inventory management, logistics planning, supplier relationships

SELECT
  p.stock_code,
  p.description,
  cat.category_name,
  SUM(f.quantity) AS total_units_sold,
  ROUND(AVG(f.unit_price), 2) AS avg_selling_price,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  COUNT(DISTINCT f.invoice_no) AS orders_containing_product
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_product p
  ON f.product_key = p.product_key
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_category cat
  ON p.category_key = cat.category_key
WHERE p._is_current = TRUE
GROUP BY p.stock_code, p.description, cat.category_name
ORDER BY total_units_sold DESC
LIMIT 10;

-- Insights to look for:
-- - Are volume leaders also revenue leaders? (Compare with previous query)
-- - Are high-volume products low-priced consumables?
-- - Do volume leaders appear in many orders (staple items)?

/*******************************************************************************
 * SECTION 5: CUSTOMER ANALYSIS
 * Purpose: Customer segmentation, lifetime value, retention insights
 ******************************************************************************/

-- ============================================================================
-- Top 10 customers by revenue (Excluding guest transactions)
-- ============================================================================
-- Business Question: Who are our most valuable customers?
-- Use Case: VIP programs, personalized marketing, retention strategies

SELECT
  c.customer_id,
  co.country_name,
  c.total_lifetime_orders,
  SUM(f.quantity) AS total_units_purchased,
  ROUND(SUM(f.total_amount), 2) AS total_lifetime_value,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value,
  COUNT(DISTINCT f.invoice_no) AS orders_in_period
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_customer c
  ON f.customer_key = c.customer_key
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country co
  ON c.country_key = co.country_key
WHERE c._is_current = TRUE
GROUP BY c.customer_id, co.country_name, c.total_lifetime_orders
ORDER BY total_lifetime_value DESC
LIMIT 10;

-- Insights to look for:
-- - Are top customers from specific countries?
-- - Do top customers have high order frequency or high avg value?
-- - Should VIP programs target high-value vs. high-frequency customers?

-- ============================================================================
-- Customer order frequency distribution
-- ============================================================================
-- Business Question: How many customers are one-time vs. repeat buyers?
-- Use Case: Retention strategy, loyalty program design, cohort analysis

SELECT
  order_count_bucket,
  COUNT(DISTINCT customer_id) AS customer_count,
  ROUND(100.0 * COUNT(DISTINCT customer_id) / SUM(COUNT(DISTINCT customer_id)) OVER (), 2) AS percent_of_customers
FROM (
  SELECT
    c.customer_id,
    c.total_lifetime_orders,
    CASE
      WHEN c.total_lifetime_orders = 1 THEN '1 order (one-time buyer)'
      WHEN c.total_lifetime_orders BETWEEN 2 AND 5 THEN '2-5 orders (occasional buyer)'
      WHEN c.total_lifetime_orders BETWEEN 6 AND 10 THEN '6-10 orders (regular buyer)'
      ELSE '11+ orders (loyal buyer)'
    END AS order_count_bucket
  FROM ECOMMERCE_DW.PRODUCTION.dim_customer c
  WHERE c._is_current = TRUE
)
GROUP BY order_count_bucket
ORDER BY MIN(total_lifetime_orders);

-- Insights to look for:
-- - What percentage are one-time buyers? (High churn indicator)
-- - Are there enough loyal customers to sustain business?
-- - Where should retention efforts focus (moving occasional to regular)?

-- ============================================================================
-- Guest vs. registered customer analysis
-- ============================================================================
-- Business Question: How much revenue comes from guest transactions?
-- Use Case: Checkout optimization, registration incentives, conversion strategy

SELECT
  CASE
    WHEN f.customer_key IS NULL THEN 'Guest Transaction'
    ELSE 'Registered Customer'
  END AS customer_type,
  COUNT(*) AS order_lines,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  ROUND(SUM(f.total_amount), 2) AS total_revenue,
  ROUND(AVG(f.total_amount), 2) AS avg_line_item_value,
  ROUND(100.0 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (), 2) AS percent_of_revenue
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
GROUP BY customer_type
ORDER BY total_revenue DESC;

-- Insights to look for:
-- - What percentage of revenue is from guest transactions?
-- - Do guests have different avg_line_item_value than registered customers?
-- - Is there opportunity to convert guests to registered customers?

/*******************************************************************************
 * SECTION 6: DATA INTEGRITY VALIDATION
 * Purpose: Ensure data quality and referential integrity
 ******************************************************************************/

-- ============================================================================
-- Negative quantity analysis (Returns/refunds)
-- ============================================================================
-- Business Question: How many returns/refunds are in the dataset?
-- Use Case: Return rate analysis, quality issues, customer satisfaction

SELECT
  CASE
    WHEN f.quantity < 0 THEN 'Returns/Refunds'
    WHEN f.quantity = 0 THEN 'Zero Quantity (Data Issue)'
    ELSE 'Normal Sales'
  END AS transaction_type,
  COUNT(*) AS row_count,
  ROUND(SUM(f.total_amount), 2) AS total_amount,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percent_of_rows
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
GROUP BY transaction_type
ORDER BY row_count DESC;

-- Expected: No zero quantities (should be filtered in staging)
-- Returns: Some negative quantities are normal (returns happen)

-- ============================================================================
-- Price outlier analysis
-- ============================================================================
-- Business Question: Are there unusual prices that need investigation?
-- Use Case: Data quality, pricing errors, fraud detection

WITH price_stats AS (
  SELECT
    MIN(unit_price)   AS min_price,
    MAX(unit_price)   AS max_price,
    AVG(unit_price)   AS avg_price,
    MEDIAN(unit_price) AS median_price
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales
)

SELECT
  'Minimum Price' AS metric,
  ps.min_price     AS value,
  COUNT(*)         AS row_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales fs
CROSS JOIN price_stats ps
WHERE fs.unit_price = ps.min_price
GROUP BY ps.min_price

UNION ALL

SELECT
  'Maximum Price' AS metric,
  ps.max_price     AS value,
  COUNT(*)         AS row_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales fs
CROSS JOIN price_stats ps
WHERE fs.unit_price = ps.max_price
GROUP BY ps.max_price

UNION ALL

SELECT
  'Average Price' AS metric,
  ROUND(ps.avg_price, 2) AS value,
  NULL AS row_count
FROM price_stats ps

UNION ALL

SELECT
  'Median Price' AS metric,
  ROUND(ps.median_price, 2) AS value,
  NULL AS row_count
FROM price_stats ps;

-- Look for: Extreme values that seem unrealistic (e.g., £10,000+ per unit)

-- ============================================================================
-- Missing dimension analysis (NULL foreign keys)
-- ============================================================================
-- Business Question: Are there any missing dimension references?
-- Use Case: Data quality monitoring, ETL validation

SELECT
  'Date Key' AS dimension,
  COUNT(*) AS total_rows,
  COUNT(date_key) AS non_null_count,
  COUNT(*) - COUNT(date_key) AS null_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales

UNION ALL

SELECT
  'Customer Key' AS dimension,
  COUNT(*) AS total_rows,
  COUNT(customer_key) AS non_null_count,
  COUNT(*) - COUNT(customer_key) AS null_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales

UNION ALL

SELECT
  'Product Key' AS dimension,
  COUNT(*) AS total_rows,
  COUNT(product_key) AS non_null_count,
  COUNT(*) - COUNT(product_key) AS null_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales

UNION ALL

SELECT
  'Country Key' AS dimension,
  COUNT(*) AS total_rows,
  COUNT(country_key) AS non_null_count,
  COUNT(*) - COUNT(country_key) AS null_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

-- Expected:
-- - Date Key: 0 NULLs (required)
-- - Customer Key: Some NULLs (guest transactions)
-- - Product Key: 0 NULLs (required)
-- - Country Key: 0 NULLs (required)

/*******************************************************************************
 * SECTION 7: BUSINESS METRICS SUMMARY
 * Purpose: Executive dashboard metrics
 ******************************************************************************/

-- ============================================================================
-- Overall business performance summary
-- ============================================================================
-- Business Question: What are the key business metrics at a glance?
-- Use Case: Executive dashboard, board reporting, performance monitoring
-- 
WITH base AS (
  SELECT
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_line_item_value,
    COUNT(*) AS total_line_items,
    COUNT(DISTINCT f.invoice_no) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    COUNT(DISTINCT f.customer_key) AS active_customers,
    SUM(CASE WHEN f.customer_key IS NULL THEN 1 ELSE 0 END) AS guest_transactions,
    COUNT(DISTINCT f.product_key) AS products_sold,
    COUNT(DISTINCT f.country_key) AS countries_served
  FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
)

SELECT
  -- Revenue Metrics
  ROUND(b.total_revenue, 2) AS total_revenue,
  ROUND(b.avg_line_item_value, 2) AS avg_line_item_value,

  -- Volume Metrics
  b.total_line_items AS total_line_items,
  b.total_orders AS total_orders,
  b.total_units_sold AS total_units_sold,

  -- Customer Metrics
  b.active_customers AS active_customers,
  b.guest_transactions AS guest_transactions,

  -- Product Metrics
  b.products_sold AS products_sold,

  -- Geographic Metrics
  b.countries_served AS countries_served,

  -- Derived Metrics (safe division)
  ROUND(b.total_revenue / NULLIF(b.total_orders, 0), 2) AS avg_order_value,
  ROUND( b.total_line_items / NULLIF(CAST(b.total_orders AS FLOAT), 0), 2) AS avg_items_per_order,
  ROUND( b.total_revenue / NULLIF(b.active_customers, 0), 2) AS revenue_per_customer

FROM base b;
-- This single query provides a comprehensive business overview

-- Data integrity check: fact total should match staging total
SELECT
  'FACT TABLE' AS source,
  SUM(total_amount) AS total_revenue
FROM ECOMMERCE_DW.PRODUCTION.fact_sales
UNION ALL
SELECT
  'STAGING TABLE' AS source,
  SUM(total_amount) AS total_revenue
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

/*******************************************************************************
 * VALIDATION SUMMARY
 *
 * Key Metrics to Validate:
 * -----------------------
 * 1. Total Revenue: Should match staging SUM(total_amount)
 * 2. Row Count: Should be ~400K-500K for this dataset
 * 3. Date Range: Should span 2009-2011
 * 4. NULL Customer Keys: Should be 20-30% (guest transactions)
 * 5. Zero Orphaned Foreign Keys: All lookups should succeed
 *
 * Business Insights to Explore:
 * -----------------------------
 * 1. Geographic concentration: Is revenue concentrated in UK?
 * 2. Seasonal patterns: Are there Q4 holiday spikes?
 * 3. Product performance: Are a few products driving most revenue?
 * 4. Customer segmentation: What's the one-time vs. loyal customer ratio?
 * 5. Pricing: Are there outliers that need investigation?
 *
 * Next Steps:
 * ----------
 * 1. Create advanced analytics queries (RFM, cohort analysis, basket analysis)
 * 2. Build visualizations in BI tool (Tableau, Power BI)
 * 3. Set up monitoring for data quality and freshness
 * 4. Implement incremental loading for ongoing data updates
 ******************************************************************************/
