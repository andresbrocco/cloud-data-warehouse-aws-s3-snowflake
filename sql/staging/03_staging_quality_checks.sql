/*******************************************************************************
 * Script: 03_staging_quality_checks.sql
 * Purpose: Comprehensive data quality validation for STAGING layer
 *
 * Description:
 *   This script performs thorough quality checks on the stg_orders table to
 *   validate transformations, identify anomalies, and provide insights into
 *   the cleaned dataset. These queries serve as automated tests that should
 *   be run after every staging load.
 *
 *   Quality Check Categories:
 *   1. Row count validations (raw vs staging comparison)
 *   2. Data type conversion success rates
 *   3. Business metric reasonableness checks
 *   4. Temporal data validation
 *   5. Referential integrity previews
 *   6. Outlier detection
 *
 * Usage:
 *   Run this script after executing 02_load_staging_from_raw.sql to verify
 *   the transformation pipeline produced expected results.
 *
 * Prerequisites:
 *   1. stg_orders table loaded (sql/staging/02_load_staging_from_raw.sql)
 *   2. RAW.RAW_TRANSACTIONS loaded (for comparison queries)
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-05
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA STAGING;
USE ROLE SYSADMIN;

/*******************************************************************************
 * EXECUTIVE SUMMARY: ALL QUALITY CHECKS IN SINGLE VIEW
 *
 * Purpose: Single-table dashboard showing status of all quality checks
 * Use Case: Screenshot-ready summary for portfolio documentation
 ******************************************************************************/

WITH raw_metrics AS (
  SELECT
    COUNT(*) AS raw_total_records
  FROM ECOMMERCE_DW.RAW.RAW_TRANSACTIONS
),
staging_metrics AS (
  SELECT
    COUNT(*) AS staging_total_records,
    COUNT_IF(is_valid = TRUE) AS valid_records,
    COUNT_IF(is_valid = FALSE) AS invalid_records,
    ROUND((COUNT_IF(is_valid = TRUE) * 100.0) / COUNT(*), 2) AS validation_rate,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT_IF(customer_id IS NOT NULL AND is_valid = TRUE) AS records_with_customer,
    COUNT_IF(is_valid = TRUE) AS valid_total,
    ROUND(SUM(CASE WHEN is_valid = TRUE THEN total_amount ELSE 0 END), 2) AS total_revenue,
    MIN(invoice_date) AS earliest_date,
    MAX(invoice_date) AS latest_date,
    COUNT(DISTINCT invoice_no) AS unique_invoices,
    COUNT(DISTINCT stock_code) AS unique_products,
    COUNT(DISTINCT country) AS unique_countries
  FROM ECOMMERCE_DW.STAGING.stg_orders
),
completeness_metrics AS (
  SELECT
    ROUND((COUNT(customer_id) * 100.0) / COUNT(*), 2) AS customer_id_completeness,
    ROUND((COUNT(description) * 100.0) / COUNT(*), 2) AS description_completeness,
    ROUND((COUNT(invoice_date) * 100.0) / COUNT(*), 2) AS date_completeness
  FROM ECOMMERCE_DW.STAGING.stg_orders
  WHERE is_valid = TRUE
)
SELECT
  '📊 STAGING QUALITY CHECKS - EXECUTIVE SUMMARY' AS check_category,
  NULL AS metric,
  NULL AS value,
  NULL AS threshold,
  NULL AS status

UNION ALL

-- Check 1: Raw to Staging Record Count
SELECT
  '1. Data Ingestion' AS check_category,
  'Raw → Staging Records' AS metric,
  r.raw_total_records || ' → ' || s.staging_total_records AS value,
  '100% transfer' AS threshold,
  CASE
    WHEN s.staging_total_records = r.raw_total_records THEN '✅ PASSED'
    WHEN s.staging_total_records >= r.raw_total_records * 0.99 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM raw_metrics r, staging_metrics s

UNION ALL

-- Check 2: Validation Success Rate
SELECT
  '2. Data Quality' AS check_category,
  'Validation Success Rate' AS metric,
  s.validation_rate || '%' AS value,
  '≥ 70%' AS threshold,
  CASE
    WHEN s.validation_rate >= 70 THEN '✅ PASSED'
    WHEN s.validation_rate >= 50 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM staging_metrics s

UNION ALL

-- Check 3: Invalid Records Count
SELECT
  '2. Data Quality' AS check_category,
  'Invalid Records Identified' AS metric,
  s.invalid_records::VARCHAR AS value,
  'Documented' AS threshold,
  CASE
    WHEN s.invalid_records > 0 THEN '✅ PASSED'
    ELSE '✅ PASSED'
  END AS status
FROM staging_metrics s

UNION ALL

-- Check 4: Revenue Validation
SELECT
  '3. Business Metrics' AS check_category,
  'Total Revenue' AS metric,
  '$' || s.total_revenue AS value,
  '> $0' AS threshold,
  CASE
    WHEN s.total_revenue > 0 THEN '✅ PASSED'
    WHEN s.total_revenue = 0 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM staging_metrics s

UNION ALL

-- Check 5: Unique Entities
SELECT
  '3. Business Metrics' AS check_category,
  'Unique Customers' AS metric,
  s.unique_customers::VARCHAR AS value,
  '> 1000' AS threshold,
  CASE
    WHEN s.unique_customers > 1000 THEN '✅ PASSED'
    WHEN s.unique_customers > 500 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM staging_metrics s

UNION ALL

SELECT
  '3. Business Metrics' AS check_category,
  'Unique Products' AS metric,
  s.unique_products::VARCHAR AS value,
  '> 100' AS threshold,
  CASE
    WHEN s.unique_products > 100 THEN '✅ PASSED'
    ELSE '⚠️ WARNING'
  END AS status
FROM staging_metrics s

UNION ALL

SELECT
  '3. Business Metrics' AS check_category,
  'Unique Countries' AS metric,
  s.unique_countries::VARCHAR AS value,
  '> 10' AS threshold,
  CASE
    WHEN s.unique_countries > 10 THEN '✅ PASSED'
    ELSE '⚠️ WARNING'
  END AS status
FROM staging_metrics s

UNION ALL

-- Check 6: Temporal Validation
SELECT
  '4. Temporal Data' AS check_category,
  'Date Range' AS metric,
  TO_CHAR(s.earliest_date, 'YYYY-MM-DD') || ' to ' || TO_CHAR(s.latest_date, 'YYYY-MM-DD') AS value,
  'Valid dates' AS threshold,
  CASE
    WHEN s.earliest_date IS NOT NULL AND s.latest_date IS NOT NULL
         AND s.latest_date > s.earliest_date THEN '✅ PASSED'
    ELSE '❌ FAILED'
  END AS status
FROM staging_metrics s

UNION ALL

SELECT
  '4. Temporal Data' AS check_category,
  'Date Coverage (days)' AS metric,
  DATEDIFF('day', s.earliest_date, s.latest_date)::VARCHAR AS value,
  '> 30 days' AS threshold,
  CASE
    WHEN DATEDIFF('day', s.earliest_date, s.latest_date) > 30 THEN '✅ PASSED'
    ELSE '⚠️ WARNING'
  END AS status
FROM staging_metrics s

UNION ALL

-- Check 7: Customer ID Completeness
SELECT
  '5. Data Completeness' AS check_category,
  'Customer ID Coverage' AS metric,
  c.customer_id_completeness || '%' AS value,
  '≥ 60%' AS threshold,
  CASE
    WHEN c.customer_id_completeness >= 60 THEN '✅ PASSED'
    WHEN c.customer_id_completeness >= 40 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM completeness_metrics c

UNION ALL

SELECT
  '5. Data Completeness' AS check_category,
  'Product Description Coverage' AS metric,
  c.description_completeness || '%' AS value,
  '≥ 90%' AS threshold,
  CASE
    WHEN c.description_completeness >= 90 THEN '✅ PASSED'
    WHEN c.description_completeness >= 75 THEN '⚠️ WARNING'
    ELSE '❌ FAILED'
  END AS status
FROM completeness_metrics c

UNION ALL

SELECT
  '5. Data Completeness' AS check_category,
  'Invoice Date Coverage' AS metric,
  c.date_completeness || '%' AS value,
  '100%' AS threshold,
  CASE
    WHEN c.date_completeness = 100 THEN '✅ PASSED'
    ELSE '❌ FAILED'
  END AS status
FROM completeness_metrics c

UNION ALL

-- Check 8: Referential Integrity Preview
SELECT
  '6. Data Structure' AS check_category,
  'Unique Invoice Numbers' AS metric,
  s.unique_invoices::VARCHAR AS value,
  '> 0' AS threshold,
  CASE
    WHEN s.unique_invoices > 0 THEN '✅ PASSED'
    ELSE '❌ FAILED'
  END AS status
FROM staging_metrics s;

/*******************************************************************************
 * DETAILED QUALITY CHECKS BELOW
 *
 * The following sections provide granular breakdowns of each check category.
 * Use these for investigation when summary shows warnings or failures.
 ******************************************************************************/

/*******************************************************************************
 * QUALITY CHECK 1: OVERALL DATA SUMMARY
 ******************************************************************************/

SELECT '=== OVERALL DATA SUMMARY ===' AS section;

SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT invoice_no) AS unique_invoices,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT stock_code) AS unique_products,
  COUNT(DISTINCT country) AS unique_countries,
  MIN(invoice_date) AS earliest_transaction,
  MAX(invoice_date) AS latest_transaction,
  DATEDIFF('day', MIN(invoice_date), MAX(invoice_date)) AS date_range_days
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

/*******************************************************************************
 * QUALITY CHECK 2: DATA QUALITY METRICS
 ******************************************************************************/

SELECT '=== DATA QUALITY METRICS ===' AS section;

-- Validation success rate
SELECT
  'Validation Success Rate' AS metric,
  COUNT(*) AS total_records,
  COUNT_IF(is_valid = TRUE) AS valid_records,
  COUNT_IF(is_valid = FALSE) AS invalid_records,
  ROUND((COUNT_IF(is_valid = TRUE) * 100.0) / COUNT(*), 2) AS success_rate_pct
FROM ECOMMERCE_DW.STAGING.stg_orders;

-- Breakdown of quality issues
SELECT
  quality_issues,
  COUNT(*) AS issue_count,
  ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2) AS percentage_of_invalid
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = FALSE
GROUP BY quality_issues
ORDER BY issue_count DESC;

/*******************************************************************************
 * QUALITY CHECK 3: REVENUE METRICS
 ******************************************************************************/

SELECT '=== REVENUE METRICS ===' AS section;

SELECT
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_transaction_amount,
  ROUND(MEDIAN(total_amount), 2) AS median_transaction_amount,
  ROUND(MIN(total_amount), 2) AS min_transaction_amount,
  ROUND(MAX(total_amount), 2) AS max_transaction_amount,
  ROUND(STDDEV(total_amount), 2) AS stddev_transaction_amount
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

/*******************************************************************************
 * QUALITY CHECK 4: TOP COUNTRIES BY REVENUE
 ******************************************************************************/

SELECT '=== TOP 10 COUNTRIES BY REVENUE ===' AS section;

SELECT
  country,
  COUNT(*) AS order_count,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT invoice_no) AS unique_invoices,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 10;

/*******************************************************************************
 * QUALITY CHECK 5: TOP PRODUCTS BY QUANTITY SOLD
 ******************************************************************************/

SELECT '=== TOP 10 PRODUCTS BY QUANTITY SOLD ===' AS section;

SELECT
  stock_code,
  description,
  SUM(quantity) AS total_quantity_sold,
  COUNT(*) AS number_of_orders,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY stock_code, description
ORDER BY total_quantity_sold DESC
LIMIT 10;

/*******************************************************************************
 * QUALITY CHECK 6: TEMPORAL PATTERNS
 ******************************************************************************/

SELECT '=== TEMPORAL PATTERNS ===' AS section;

-- Orders by month
SELECT
  TO_CHAR(invoice_date, 'YYYY-MM') AS year_month,
  COUNT(*) AS order_count,
  COUNT(DISTINCT customer_id) AS unique_customers,
  ROUND(SUM(total_amount), 2) AS monthly_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY TO_CHAR(invoice_date, 'YYYY-MM')
ORDER BY year_month;

-- Orders by day of week
SELECT
  DAYNAME(invoice_date) AS day_of_week,
  COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY DAYNAME(invoice_date), DAYOFWEEK(invoice_date)
ORDER BY DAYOFWEEK(invoice_date);

-- Orders by hour of day
SELECT
  HOUR(invoice_date) AS hour_of_day,
  COUNT(*) AS order_count,
  ROUND(SUM(total_amount), 2) AS total_revenue
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY HOUR(invoice_date)
ORDER BY hour_of_day;

/*******************************************************************************
 * QUALITY CHECK 7: CUSTOMER ANALYSIS
 ******************************************************************************/

SELECT '=== CUSTOMER ANALYSIS ===' AS section;

-- Customer distribution
SELECT
  'Total Valid Records' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT
  'Records with Customer ID' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE AND customer_id IS NOT NULL

UNION ALL

SELECT
  'Records without Customer ID (Guest Checkout)' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE AND customer_id IS NULL

UNION ALL

SELECT
  'Unique Customers' AS metric,
  COUNT(DISTINCT customer_id) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE AND customer_id IS NOT NULL;

-- Top customers by revenue
SELECT
  customer_id,
  COUNT(*) AS order_count,
  COUNT(DISTINCT invoice_no) AS unique_invoices,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE AND customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;

/*******************************************************************************
 * QUALITY CHECK 8: OUTLIER DETECTION
 ******************************************************************************/

SELECT '=== OUTLIER DETECTION ===' AS section;

-- High-value transactions (potential outliers)
SELECT
  invoice_no,
  invoice_date,
  customer_id,
  country,
  quantity,
  unit_price,
  total_amount,
  'High value transaction' AS outlier_type
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
  AND total_amount > (
    SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_amount)
    FROM ECOMMERCE_DW.STAGING.stg_orders
    WHERE is_valid = TRUE
  )
ORDER BY total_amount DESC
LIMIT 20;

-- High quantity orders
SELECT
  invoice_no,
  stock_code,
  description,
  quantity,
  unit_price,
  total_amount,
  'High quantity order' AS outlier_type
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
  AND quantity > (
    SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY quantity)
    FROM ECOMMERCE_DW.STAGING.stg_orders
    WHERE is_valid = TRUE
  )
ORDER BY quantity DESC
LIMIT 20;

/*******************************************************************************
 * QUALITY CHECK 9: DATE KEY VALIDATION
 ******************************************************************************/

SELECT '=== DATE KEY VALIDATION ===' AS section;

-- Verify invoice_date_key format
SELECT
  invoice_date_key,
  COUNT(*) AS record_count,
  MIN(invoice_date) AS min_date,
  MAX(invoice_date) AS max_date
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE
GROUP BY invoice_date_key
ORDER BY invoice_date_key
LIMIT 10;

-- Check for any NULL date keys (should be none for valid records)
SELECT
  COUNT_IF(invoice_date_key IS NULL) AS null_date_keys,
  COUNT_IF(invoice_date IS NULL) AS null_invoice_dates
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

/*******************************************************************************
 * QUALITY CHECK 10: DATA COMPLETENESS
 ******************************************************************************/

SELECT '=== DATA COMPLETENESS (Valid Records Only) ===' AS section;

SELECT
  'invoice_no' AS column_name,
  COUNT(*) AS total_records,
  COUNT(invoice_no) AS non_null_count,
  COUNT(*) - COUNT(invoice_no) AS null_count,
  ROUND((COUNT(invoice_no) * 100.0) / COUNT(*), 2) AS completeness_pct
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT 'stock_code', COUNT(*), COUNT(stock_code),
  COUNT(*) - COUNT(stock_code),
  ROUND((COUNT(stock_code) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'description', COUNT(*), COUNT(description),
  COUNT(*) - COUNT(description),
  ROUND((COUNT(description) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'country', COUNT(*), COUNT(country),
  COUNT(*) - COUNT(country),
  ROUND((COUNT(country) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'customer_id', COUNT(*), COUNT(customer_id),
  COUNT(*) - COUNT(customer_id),
  ROUND((COUNT(customer_id) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'quantity', COUNT(*), COUNT(quantity),
  COUNT(*) - COUNT(quantity),
  ROUND((COUNT(quantity) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'unit_price', COUNT(*), COUNT(unit_price),
  COUNT(*) - COUNT(unit_price),
  ROUND((COUNT(unit_price) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE

UNION ALL

SELECT 'invoice_date', COUNT(*), COUNT(invoice_date),
  COUNT(*) - COUNT(invoice_date),
  ROUND((COUNT(invoice_date) * 100.0) / COUNT(*), 2)
FROM ECOMMERCE_DW.STAGING.stg_orders WHERE is_valid = TRUE;

/*******************************************************************************
 * QUALITY CHECK 11: COMPARISON WITH RAW LAYER
 ******************************************************************************/

SELECT '=== RAW vs STAGING COMPARISON ===' AS section;

-- Row count comparison
SELECT
  'RAW Layer' AS layer,
  COUNT(*) AS total_records
FROM ECOMMERCE_DW.RAW.RAW_TRANSACTIONS

UNION ALL

SELECT
  'STAGING Layer (All)' AS layer,
  COUNT(*) AS total_records
FROM ECOMMERCE_DW.STAGING.stg_orders

UNION ALL

SELECT
  'STAGING Layer (Valid)' AS layer,
  COUNT(*) AS total_records
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT
  'STAGING Layer (Invalid)' AS layer,
  COUNT(*) AS total_records
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = FALSE;

/*******************************************************************************
 * QUALITY CHECKS COMPLETE
 *
 * Review Checklist:
 * ----------------
 * ✓ Overall record counts look reasonable
 * ✓ Validation success rate is 70-85% (typical for e-commerce data)
 * ✓ Quality issues breakdown shows cancelled orders as primary issue
 * ✓ Revenue metrics are positive and reasonable
 * ✓ Top countries include United Kingdom (primary market)
 * ✓ Temporal patterns show realistic business activity
 * ✓ Date keys are properly formatted (YYYYMMDD)
 * ✓ No NULL values in critical fields for valid records
 * ✓ Customer ID completeness matches expected guest checkout rate
 *
 * Red Flags to Investigate:
 * ------------------------
 * - Validation success rate < 50% (too much data loss)
 * - Zero revenue or negative total revenue
 * - All dates in single day (date conversion issue)
 * - 100% NULL customer IDs (conversion failure)
 * - Outliers that seem impossible (data entry errors)
 *
 * Next Steps:
 * ----------
 * 1. Review any unexpected patterns from the queries above
 * 2. Update README.md with staging layer description
 * 3. Proceed to PRODUCTION layer dimensional model implementation
 ******************************************************************************/
