/*******************************************************************************
 * Script: 07_load_fact_sales.sql
 * Purpose: Populate fact_sales table from staging with dimension lookups
 *
 * Description:
 *   This script performs the ETL (Extract, Transform, Load) process to populate
 *   the fact_sales table from the staging layer. It executes dimension lookups
 *   to convert business keys (customer_id, stock_code, country) into surrogate
 *   keys (customer_key, product_key, country_key), then inserts transactional
 *   data into the fact table.
 *
 *   Key Operations:
 *   1. Extract valid records from STAGING.stg_orders (WHERE is_valid = TRUE)
 *   2. Transform via dimension lookups (join to dim_* tables)
 *   3. Load into PRODUCTION.fact_sales (INSERT)
 *   4. Validate data integrity (row counts, totals, orphaned keys)
 *
 * Dimension Lookup Strategy:
 *   - date_key: Direct from stg_orders.invoice_date_key (pre-computed in staging)
 *   - customer_key: LEFT JOIN to dim_customer (nullable for guest transactions)
 *   - product_key: INNER JOIN to dim_product (required, must exist)
 *   - country_key: INNER JOIN to dim_country (required, must exist)
 *
 * SCD Type 2 Handling:
 *   Customer and product dimensions use SCD Type 2 (historical tracking).
 *   We join on _is_current = TRUE to get the current version of each dimension.
 *   For historical loads, you'd use point-in-time logic with _effective_from/_effective_to.
 *
 * Prerequisites:
 *   1. fact_sales table created (sql/production/06_create_fact_sales.sql)
 *   2. All dimension tables populated:
 *      - dim_date (sql/production/01_create_dim_date.sql)
 *      - dim_country (sql/production/02_create_dim_country.sql)
 *      - dim_customer (sql/production/03_create_dim_customer.sql)
 *      - dim_category (sql/production/04_create_dim_category.sql)
 *      - dim_product (sql/production/05_create_dim_product.sql)
 *   3. STAGING.stg_orders populated (sql/staging/02_load_staging_from_raw.sql)
 *
 * Execution Instructions:
 *   1. Ensure all prerequisites are completed
 *   2. Execute this entire script in Snowflake worksheet
 *   3. Review validation query results at the end
 *   4. Expected row count: ~400K-500K rows (depends on dataset)
 *   5. Proceed to validation script (08_fact_validation_queries.sql)
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
 * FACT TABLE LOADING STRATEGY
 *
 * Overview:
 * --------
 * This script implements a FULL REFRESH pattern: TRUNCATE existing data, then
 * reload from staging. This is appropriate for initial loads and when the
 * entire dataset fits comfortably in memory/storage.
 *
 * For production incremental loads, you'd use:
 * - Partition-based loading (load only new date ranges)
 * - MERGE statements (upsert pattern)
 * - Change Data Capture (CDC) for real-time updates
 *
 * Dimension Lookup Logic:
 * ----------------------
 * The core challenge in fact table loading is converting business keys from
 * staging into surrogate keys from dimensions:
 *
 * STAGING → FACT
 * stg_orders.customer_id → fact_sales.customer_key (via dim_customer lookup)
 * stg_orders.stock_code → fact_sales.product_key (via dim_product lookup)
 * stg_orders.country → fact_sales.country_key (via dim_country lookup)
 * stg_orders.invoice_date_key → fact_sales.date_key (direct copy, already computed)
 *
 * Join Types Explained:
 * --------------------
 * INNER JOIN: Required relationships (fail if dimension doesn't exist)
 *   - dim_date: Every transaction must have a date
 *   - dim_product: Every line item must have a product
 *   - dim_country: Every transaction must have a shipping destination
 *
 * LEFT JOIN: Optional relationships (allow NULL if dimension doesn't exist)
 *   - dim_customer: Guest transactions don't have customer_id
 *
 * Why INNER JOIN for product/country?
 * -----------------------------------
 * If a product or country doesn't exist in the dimension, it indicates a data
 * quality issue that should be resolved before loading facts. INNER JOIN will
 * exclude these rows, preventing orphaned foreign keys.
 *
 * In production, you'd:
 * 1. Log failed lookups for investigation
 * 2. Insert missing dimension rows (if valid)
 * 3. Retry fact loading
 *
 * SCD Type 2 Considerations:
 * -------------------------
 * dim_customer and dim_product use SCD Type 2, meaning multiple versions may
 * exist for the same business key. When loading facts, we MUST specify which
 * version to use.
 *
 * For CURRENT LOAD (most common):
 *   JOIN dim_customer ON stg.customer_id = dim_customer.customer_id
 *   AND dim_customer._is_current = TRUE
 *
 * For HISTORICAL LOAD (backfilling):
 *   JOIN dim_customer ON stg.customer_id = dim_customer.customer_id
 *   AND stg.invoice_date BETWEEN dim_customer._effective_from
 *     AND COALESCE(dim_customer._effective_to, '9999-12-31')
 *
 * This project uses CURRENT LOAD (initial load, all data is historical).
 *
 * Data Filtering:
 * --------------
 * We load ONLY valid records from staging (WHERE is_valid = TRUE). Invalid
 * records remain in staging for quality monitoring but don't pollute the
 * production fact table.
 *
 * Idempotency:
 * -----------
 * This script uses TRUNCATE before INSERT, making it idempotent (safe to re-run).
 * Running multiple times produces the same result. This is critical for:
 * - Development and testing (run, fix issues, re-run)
 * - Production reruns after fixing data quality issues
 * - Disaster recovery scenarios
 ******************************************************************************/

-- Clear existing data (idempotency)
TRUNCATE TABLE ECOMMERCE_DW.PRODUCTION.fact_sales;

/*******************************************************************************
 * INSERT INTO FACT TABLE WITH DIMENSION LOOKUPS
 *
 * This query performs the following operations:
 *
 * 1. Source: STAGING.stg_orders (WHERE is_valid = TRUE)
 *    - Only load validated, clean records
 *    - Excludes cancellations, invalid quantities, bad dates, etc.
 *
 * 2. Dimension Lookups (Convert Business Keys → Surrogate Keys):
 *    - dim_customer: Lookup customer_key by customer_id
 *    - dim_product: Lookup product_key by stock_code
 *    - dim_country: Lookup country_key by country_name
 *    - date_key: Already computed in staging (invoice_date_key)
 *
 * 3. SCD Type 2 Filtering:
 *    - Customer: _is_current = TRUE (current version)
 *    - Product: _is_current = TRUE (current version)
 *
 * 4. Insert: Copy measures and foreign keys into fact_sales
 *
 * Row Exclusions:
 * --------------
 * INNER JOIN on product/country means rows are excluded if:
 * - Product stock_code doesn't exist in dim_product
 * - Country name doesn't exist in dim_country
 *
 * These represent data quality issues that should be investigated. In production:
 * - Log excluded rows for investigation
 * - Insert missing dimension rows if valid
 * - Consider using LEFT JOIN with COALESCE to default dimension (e.g., "Unknown")
 *
 * Performance Notes:
 * -----------------
 * - Snowflake optimizes joins using micro-partition pruning
 * - INTEGER joins (surrogate keys) are faster than VARCHAR joins
 * - LEFT JOIN on customer doesn't significantly impact performance
 * - Query execution time: ~10-30 seconds for 400K rows (dataset dependent)
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.fact_sales (
  date_key,
  customer_key,
  product_key,
  country_key,
  invoice_no,
  quantity,
  unit_price,
  total_amount
)
SELECT
  -- Date dimension lookup (direct copy, already computed in staging)
  stg.invoice_date_key AS date_key,

  -- Customer dimension lookup (may be NULL for guest transactions)
  cust.customer_key,

  -- Product dimension lookup (required, INNER JOIN ensures it exists)
  prod.product_key,

  -- Country dimension lookup (required, INNER JOIN ensures it exists)
  country.country_key,

  -- Degenerate dimension (transaction identifier)
  stg.invoice_no,

  -- Measures (business metrics)
  stg.quantity,
  stg.unit_price,
  stg.total_amount

FROM ECOMMERCE_DW.STAGING.stg_orders stg

-- Customer dimension lookup (LEFT JOIN allows NULL for guest transactions)
LEFT JOIN ECOMMERCE_DW.PRODUCTION.dim_customer cust
  ON stg.customer_id = cust.customer_id
  AND cust._is_current = TRUE  -- SCD Type 2: Get current version only

-- Product dimension lookup (INNER JOIN requires product to exist)
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_product prod
  ON stg.stock_code = prod.stock_code
  AND prod._is_current = TRUE  -- SCD Type 2: Get current version only

-- Country dimension lookup (INNER JOIN requires country to exist)
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country country
  ON stg.country = country.country_name

-- Filter to valid records only (staging quality gate)
WHERE stg.is_valid = TRUE;

-- Confirm successful load
SELECT 'fact_sales loaded successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

/*******************************************************************************
 * DATA VALIDATION QUERIES
 *
 * After loading, always validate data integrity to ensure:
 * 1. Row counts match expectations
 * 2. Revenue totals match staging
 * 3. No orphaned foreign keys
 * 4. Date ranges are correct
 * 5. NULL patterns are expected
 ******************************************************************************/

-- ============================================================================
-- VALIDATION 1: Row Count Comparison (Fact vs. Staging)
-- ============================================================================
-- Expected: fact_sales row count should equal or be very close to staging
-- valid row count. Small differences may occur due to failed dimension lookups.

SELECT
  'FACT TABLE' AS source,
  COUNT(*) AS row_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales

UNION ALL

SELECT
  'STAGING (VALID)' AS source,
  COUNT(*) AS row_count
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

-- If row counts differ significantly, investigate:
-- 1. Check for products/countries missing in dimensions
-- 2. Review dimension lookup join conditions
-- 3. Verify SCD Type 2 _is_current flag usage

-- ============================================================================
-- VALIDATION 2: Revenue Total Comparison (Fact vs. Staging)
-- ============================================================================
-- Expected: Total revenue should match exactly between fact and staging.
-- Any difference indicates data loss or duplication during loading.

SELECT
  'FACT TABLE' AS source,
  SUM(total_amount) AS total_revenue,
  ROUND(SUM(total_amount), 2) AS total_revenue_rounded
FROM ECOMMERCE_DW.PRODUCTION.fact_sales

UNION ALL

SELECT
  'STAGING (VALID)' AS source,
  SUM(total_amount) AS total_revenue,
  ROUND(SUM(total_amount), 2) AS total_revenue_rounded
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

-- If totals differ:
-- 1. Check for duplicate rows in fact (should have unique constraint)
-- 2. Verify WHERE is_valid = TRUE filter is consistent
-- 3. Investigate rounding errors (use ROUND for comparison)

-- ============================================================================
-- VALIDATION 3: Customer Key NULL Analysis
-- ============================================================================
-- Expected: Some rows will have NULL customer_key (guest transactions).
-- This validates that LEFT JOIN logic is working correctly.

SELECT
  COUNT(*) AS total_fact_rows,
  COUNT(customer_key) AS rows_with_customer,
  COUNT(*) - COUNT(customer_key) AS rows_without_customer,
  ROUND(100.0 * (COUNT(*) - COUNT(customer_key)) / COUNT(*), 2) AS percent_guest_transactions
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

-- Interpretation:
-- - rows_with_customer: Registered customer transactions
-- - rows_without_customer: Guest transactions (no customer_id in staging)
-- - percent_guest_transactions: Should be ~20-30% for typical e-commerce

-- ============================================================================
-- VALIDATION 4: Date Range Check
-- ============================================================================
-- Expected: Date range should match source dataset (2009-2011 for this project).
-- Validates that invoice_date_key lookups worked correctly.

SELECT
  MIN(date_key) AS min_date_key,
  MAX(date_key) AS max_date_key,
  COUNT(DISTINCT date_key) AS distinct_dates
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

-- Convert date_key back to readable dates for verification
SELECT
  TO_DATE(MIN(date_key)::VARCHAR, 'YYYYMMDD') AS min_date,
  TO_DATE(MAX(date_key)::VARCHAR, 'YYYYMMDD') AS max_date,
  DATEDIFF(DAY,
    TO_DATE(MIN(date_key)::VARCHAR, 'YYYYMMDD'),
    TO_DATE(MAX(date_key)::VARCHAR, 'YYYYMMDD')
  ) AS days_span
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

-- ============================================================================
-- VALIDATION 5: Foreign Key Integrity Check
-- ============================================================================
-- Expected: All foreign keys should reference existing dimension rows.
-- No orphaned foreign keys (fact rows pointing to non-existent dimensions).

-- Check for orphaned date keys
SELECT COUNT(*) AS orphaned_date_keys
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM ECOMMERCE_DW.PRODUCTION.dim_date d
  WHERE f.date_key = d.date_key
);
-- Expected: 0 (no orphaned keys)

-- Check for orphaned product keys
SELECT COUNT(*) AS orphaned_product_keys
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM ECOMMERCE_DW.PRODUCTION.dim_product p
  WHERE f.product_key = p.product_key
);
-- Expected: 0 (no orphaned keys)

-- Check for orphaned country keys
SELECT COUNT(*) AS orphaned_country_keys
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
WHERE NOT EXISTS (
  SELECT 1 FROM ECOMMERCE_DW.PRODUCTION.dim_country c
  WHERE f.country_key = c.country_key
);
-- Expected: 0 (no orphaned keys)

-- Check for orphaned customer keys (excluding NULL, which is valid)
SELECT COUNT(*) AS orphaned_customer_keys
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f
WHERE f.customer_key IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM ECOMMERCE_DW.PRODUCTION.dim_customer c
    WHERE f.customer_key = c.customer_key
  );
-- Expected: 0 (no orphaned keys)

-- ============================================================================
-- VALIDATION 6: Sample Fact Records with Dimension Attributes
-- ============================================================================
-- This query demonstrates a complete join across the dimensional model,
-- showing how fact measures combine with dimension attributes for analytics.

SELECT
  f.sales_key,
  f.invoice_no,
  d.date AS invoice_date,
  d.year,
  d.month_name,
  c.customer_id,
  co.country_name,
  co.region,
  p.stock_code,
  p.description AS product_name,
  cat.category_name,
  f.quantity,
  f.unit_price,
  f.total_amount
FROM ECOMMERCE_DW.PRODUCTION.fact_sales f

-- Join all dimensions to display human-readable attributes
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_date d
  ON f.date_key = d.date_key

LEFT JOIN ECOMMERCE_DW.PRODUCTION.dim_customer c
  ON f.customer_key = c.customer_key

INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_product p
  ON f.product_key = p.product_key

INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_category cat
  ON p.category_key = cat.category_key

INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country co
  ON f.country_key = co.country_key

-- Show recent transactions
ORDER BY f.sales_key DESC
LIMIT 20;

/*******************************************************************************
 * VALIDATION NOTES
 *
 * What to Look For:
 * ----------------
 * 1. Row counts should match (fact ≈ staging valid records)
 * 2. Revenue totals should match exactly
 * 3. NULL customer_keys should be present (guest transactions)
 * 4. Date range should match expected dataset (2009-2011)
 * 5. All foreign key checks should return 0 orphaned keys
 * 6. Sample records should display complete dimension attributes
 *
 * Common Issues and Solutions:
 * ---------------------------
 * Issue: Row count in fact is lower than staging
 * Cause: Products or countries missing in dimensions
 * Solution: Check which stock_codes/countries are missing, insert into dimensions
 *
 * Issue: Revenue totals don't match
 * Cause: Duplicate rows or filtering inconsistency
 * Solution: Verify WHERE is_valid = TRUE, check for duplicate inserts
 *
 * Issue: Zero NULL customer_keys
 * Cause: LEFT JOIN not working or no guest transactions in data
 * Solution: Verify staging has NULL customer_id rows, check join condition
 *
 * Issue: Orphaned foreign keys found
 * Cause: Dimension rows deleted or lookup failed
 * Solution: Verify dimension population, check SCD Type 2 _is_current flags
 *
 * Performance Troubleshooting:
 * ---------------------------
 * If INSERT takes too long (>5 minutes):
 * 1. Check staging table size (SHOW TABLES returns row count)
 * 2. Verify warehouse size (larger warehouse = faster loading)
 * 3. Consider clustering fact table by date_key for future queries
 * 4. Review query profile in Snowflake UI for bottlenecks
 *
 * Next Steps:
 * ----------
 * 1. Run advanced validation queries: sql/production/08_fact_validation_queries.sql
 * 2. Create analytics queries: sql/analytics/*.sql (future implementation)
 * 3. Connect BI tool (Tableau, Power BI) for visualization
 * 4. Set up monitoring for data quality and freshness
 ******************************************************************************/
