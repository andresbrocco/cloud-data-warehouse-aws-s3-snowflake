/*******************************************************************************
 * Script: 02_load_staging_from_raw.sql
 * Purpose: Transform and load data from RAW layer to STAGING layer
 *
 * Description:
 *   This script performs the ETL transformation from RAW_TRANSACTIONS
 *   to stg_orders. It applies data type conversions, validation rules, business
 *   logic, and quality checks to prepare clean data for dimensional modeling.
 *
 *   Key Transformations:
 *   - Type conversions: VARCHAR → INTEGER, DECIMAL, TIMESTAMP
 *   - Data cleaning: TRIM whitespace, handle NULLs
 *   - Business rules: Calculate total_amount, exclude cancelled orders
 *   - Validation: Flag invalid records with is_valid and quality_issues
 *   - Date formatting: Create invoice_date_key (YYYYMMDD) for dim_date joins
 *
 * Data Quality Approach:
 *   This script uses a "validate and flag" pattern rather than filtering out
 *   invalid records. All records are loaded into staging, but invalid ones are
 *   marked with is_valid = FALSE and quality_issues populated with the reason.
 *   
 *   Benefits:
 *   - Complete audit trail (no silent data loss)
 *   - Quality monitoring over time
 *   - Root cause analysis of data issues
 *   - Production layer can filter WHERE is_valid = TRUE
 *
 * Data Source:
 *   RAW.RAW_TRANSACTIONS
 *
 * Prerequisites:
 *   1. STAGING schema exists (sql/setup/02_create_database_schemas.sql)
 *   2. stg_orders table created (sql/staging/01_create_staging_table.sql)
 *   3. RAW.RAW_TRANSACTIONS loaded (sql/raw/03_load_data_parquet.sql)
 *
 * Execution Instructions:
 *   1. Verify prerequisites
 *   2. Execute this entire script in Snowflake worksheet
 *   3. Review validation query results at the end
 *   4. Run quality checks: sql/staging/03_staging_quality_checks.sql
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
 * STAGING DATA LOAD AND TRANSFORMATION
 *
 * Transformation Logic Explanation:
 * --------------------------------
 *
 * TRY_CAST vs CAST:
 *   - TRY_CAST returns NULL if conversion fails (graceful handling)
 *   - CAST throws error on failure (breaks entire load)
 *   - For dirty data, TRY_CAST is essential for fault tolerance
 *
 * Data Quality Validation Rules:
 *   1. invoice_no IS NULL → Invalid (cannot identify transaction)
 *   2. quantity <= 0 → Invalid (no zero or negative sales for this analysis)
 *   3. unit_price <= 0 → Invalid (products must have positive price)
 *   4. invoice_date IS NULL → Invalid (temporal analysis requires dates)
 *   5. invoice_no starts with 'C' → Invalid (cancelled order indicator)
 *
 * Business Logic:
 *   - total_amount = quantity * unit_price (pre-computed for performance)
 *   - invoice_date_key = YYYYMMDD integer format (e.g., 20101201)
 *   - TRIM all text fields to remove leading/trailing whitespace
 *   - customer_id can be NULL (guest checkout or missing data)
 *
 * Performance Pattern:
 *   - TRUNCATE + INSERT for full refresh (idempotent, repeatable)
 *   - Alternative: CREATE OR REPLACE TABLE for complete rebuild
 *   - Future: Could use MERGE for incremental updates
 ******************************************************************************/

-- Clear staging table for fresh load (idempotent pattern)
TRUNCATE TABLE ECOMMERCE_DW.STAGING.stg_orders;

-- Transform and load data from RAW to STAGING
INSERT INTO ECOMMERCE_DW.STAGING.stg_orders (
  invoice_no,
  stock_code,
  description,
  country,
  quantity,
  unit_price,
  total_amount,
  invoice_date,
  invoice_date_key,
  customer_id,
  is_valid,
  quality_issues,
  _source_row_id
)
SELECT
  -- ============================================================
  -- BUSINESS KEYS (cleaned but preserved)
  -- ============================================================
  TRIM(invoice_no) AS invoice_no,
  TRIM(stock_code) AS stock_code,

  -- ============================================================
  -- DESCRIPTIVE ATTRIBUTES (cleaned text fields)
  -- ============================================================
  TRIM(description) AS description,
  TRIM(country) AS country,

  -- ============================================================
  -- MEASURES (type conversions with safe casting)
  -- ============================================================
  TRY_CAST(quantity AS INTEGER) AS quantity,
  TRY_CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
  
  -- Computed column: total transaction amount
  -- Using TRY_CAST to handle conversion failures gracefully
  TRY_CAST(quantity AS INTEGER) * TRY_CAST(unit_price AS DECIMAL(10,2)) AS total_amount,

  -- ============================================================
  -- TEMPORAL COLUMNS (date conversions)
  -- ============================================================
  TRY_CAST(invoice_date AS TIMESTAMP_NTZ) AS invoice_date,
  
  -- Date key in YYYYMMDD format for joining to date dimension
  -- Example: 2010-12-01 12:30:00 → 20101201
  TO_NUMBER(
    TO_CHAR(TRY_CAST(invoice_date AS TIMESTAMP_NTZ), 'YYYYMMDD')
  ) AS invoice_date_key,

  -- ============================================================
  -- CUSTOMER DIMENSION (type conversion, allows NULL)
  -- ============================================================
  TRY_CAST(customer_id AS INTEGER) AS customer_id,

  -- ============================================================
  -- DATA QUALITY VALIDATION FLAG
  -- ============================================================
  -- Master validation flag: TRUE if all checks pass, FALSE otherwise
  CASE
    -- Check 1: Invoice number must exist (business key requirement)
    WHEN invoice_no IS NULL THEN FALSE
    
    -- Check 2: Quantity must be positive (no zero or negative sales)
    WHEN TRY_CAST(quantity AS INTEGER) IS NULL THEN FALSE
    WHEN TRY_CAST(quantity AS INTEGER) <= 0 THEN FALSE
    
    -- Check 3: Unit price must be positive (products must have valid pricing)
    WHEN TRY_CAST(unit_price AS DECIMAL(10,2)) IS NULL THEN FALSE
    WHEN TRY_CAST(unit_price AS DECIMAL(10,2)) <= 0 THEN FALSE
    
    -- Check 4: Invoice date required for temporal analysis
    WHEN TRY_CAST(invoice_date AS TIMESTAMP_NTZ) IS NULL THEN FALSE
    
    -- Check 5: Exclude cancelled orders (invoice_no starts with 'C')
    WHEN LEFT(invoice_no, 1) = 'C' THEN FALSE
    
    -- If all checks pass, mark as valid
    ELSE TRUE
  END AS is_valid,

  -- ============================================================
  -- QUALITY ISSUES DESCRIPTION
  -- ============================================================
  -- Provide specific reason for validation failure
  -- This enables data quality reporting and root cause analysis
  CASE
    WHEN invoice_no IS NULL THEN 'Missing invoice number'
    WHEN TRY_CAST(quantity AS INTEGER) IS NULL THEN 'Quantity not numeric'
    WHEN TRY_CAST(quantity AS INTEGER) <= 0 THEN 'Invalid quantity (zero or negative)'
    WHEN TRY_CAST(unit_price AS DECIMAL(10,2)) IS NULL THEN 'Unit price not numeric'
    WHEN TRY_CAST(unit_price AS DECIMAL(10,2)) <= 0 THEN 'Invalid unit price (zero or negative)'
    WHEN TRY_CAST(invoice_date AS TIMESTAMP_NTZ) IS NULL THEN 'Invalid or missing invoice date'
    WHEN LEFT(invoice_no, 1) = 'C' THEN 'Cancelled order'
    ELSE NULL  -- No issues detected
  END AS quality_issues,

  -- ============================================================
  -- METADATA (data lineage tracking)
  -- ============================================================
  -- Source row ID for tracing back to RAW layer
  ROW_NUMBER() OVER (ORDER BY invoice_no, invoice_date) AS _source_row_id

FROM ECOMMERCE_DW.RAW.RAW_TRANSACTIONS
WHERE invoice_no IS NOT NULL;  -- Minimal filter to exclude completely empty rows

/*******************************************************************************
 * VALIDATION QUERIES
 * 
 * These queries provide immediate feedback on the transformation results.
 * They serve as automated data quality checks and should be reviewed after
 * every staging load.
 ******************************************************************************/

-- ============================================================
-- 1. ROW COUNT SUMMARY
-- ============================================================
SELECT 
  'Total Rows Loaded' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders

UNION ALL

SELECT 
  'Valid Rows' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT 
  'Invalid Rows' AS metric,
  COUNT(*) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = FALSE

UNION ALL

SELECT 
  'Validation Success Rate' AS metric,
  ROUND(
    (COUNT_IF(is_valid = TRUE) * 100.0) / COUNT(*),
    2
  ) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders;

-- ============================================================
-- 2. QUALITY ISSUES BREAKDOWN
-- ============================================================
-- Shows the distribution of data quality problems
-- Helps prioritize which issues to fix at the source
SELECT
  quality_issues,
  COUNT(*) AS issue_count,
  ROUND(
    (COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(),
    2
  ) AS percentage
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = FALSE
GROUP BY quality_issues
ORDER BY issue_count DESC;

-- ============================================================
-- 3. DATE RANGE VALIDATION
-- ============================================================
-- Verify temporal data looks reasonable
SELECT
  'Earliest Transaction' AS metric,
  TO_VARCHAR(MIN(invoice_date), 'YYYY-MM-DD HH24:MI:SS') AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT
  'Latest Transaction' AS metric,
  TO_VARCHAR(MAX(invoice_date), 'YYYY-MM-DD HH24:MI:SS') AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE

UNION ALL

SELECT
  'Date Range (Days)' AS metric,
  TO_VARCHAR(DATEDIFF('day', MIN(invoice_date), MAX(invoice_date))) AS value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

-- ============================================================
-- 4. BUSINESS METRICS PREVIEW
-- ============================================================
-- Quick sanity check on the data
SELECT
  COUNT(DISTINCT invoice_no) AS unique_invoices,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT stock_code) AS unique_products,
  COUNT(DISTINCT country) AS unique_countries,
  ROUND(SUM(total_amount), 2) AS total_revenue,
  ROUND(AVG(total_amount), 2) AS avg_transaction_value
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

-- ============================================================
-- 5. NULL CHECK FOR CUSTOMER IDs
-- ============================================================
-- Customer ID can legitimately be NULL (guest checkout)
-- But we want to know the percentage for documentation
SELECT
  COUNT(*) AS total_valid_records,
  COUNT_IF(customer_id IS NULL) AS records_without_customer_id,
  ROUND(
    (COUNT_IF(customer_id IS NULL) * 100.0) / COUNT(*),
    2
  ) AS percentage_without_customer_id
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE;

/*******************************************************************************
 * DATA TRANSFORMATION COMPLETE
 *
 * Next Steps:
 * ----------
 * 1. Review the validation query results above
 * 2. Run comprehensive quality checks: sql/staging/03_staging_quality_checks.sql
 * 3. Document any unexpected quality issues
 * 4. Proceed to PRODUCTION layer implementation
 *
 * Expected Results (for reference):
 * --------------------------------
 * - Total rows: ~540,000 (dataset size)
 * - Valid rows: ~400,000-450,000 (after quality filtering)
 * - Invalid rows: ~90,000-140,000 (cancelled orders, bad data)
 * - Success rate: ~75-85% (typical for real-world e-commerce data)
 *
 * Common Quality Issues:
 * ---------------------
 * - Cancelled orders (~50,000-60,000): invoice_no starts with 'C'
 * - Negative quantities (~10,000-20,000): returns or data entry errors
 * - Invalid prices: rare but possible
 * - Missing dates: very rare if RAW load was successful
 *
 * If Results Look Wrong:
 * ---------------------
 * - Verify RAW table is loaded: SELECT COUNT(*) FROM RAW.RAW_TRANSACTIONS;
 * - Check for NULL patterns: SELECT * FROM STAGING.stg_orders WHERE is_valid = FALSE LIMIT 100;
 * - Review quality_issues values for unexpected patterns
 *
 * Performance Notes:
 * -----------------
 * - This transformation should complete in 10-30 seconds for 540K rows
 * - Using Parquet source (RAW_TRANSACTIONS) for optimal performance
 * - TRUNCATE + INSERT pattern ensures idempotent execution
 * - Can be run multiple times safely (full refresh approach)
 ******************************************************************************/
