/*******************************************************************************
 * Script: 03_load_data_parquet.sql
 * Purpose: Load e-commerce transaction data from Parquet files in S3
 *
 * Description:
 *   This script demonstrates loading Parquet format data into Snowflake.
 *   Parquet is a columnar storage format that offers significant advantages
 *   over CSV for data lake architectures:
 *     - 70-90% smaller file size (better compression)
 *     - Faster load times (columnar format, parallel processing)
 *     - Built-in schema (reduces parsing errors)
 *     - Better for large-scale data workloads
 *
 *   This script mirrors 02_load_data_csv.sql structure but uses PARQUET_FORMAT
 *   instead of CSV_FORMAT. The comparison between these two approaches helps
 *   demonstrate why Parquet is preferred for cloud data lakes.
 *
 * Loading Strategy:
 *   - Uses same external stage as CSV load
 *   - Leverages PARQUET_FORMAT defined in 03_create_file_formats.sql
 *   - Loads into same raw_transactions table (for comparison)
 *   - Captures same metadata for data lineage
 *
 * Prerequisites:
 *   1. External stage configured (sql/setup/04_create_external_stage.sql)
 *   2. Raw table created (sql/raw/01_create_raw_table.sql)
 *   3. Parquet file uploaded to S3 bucket (online_retail.parquet)
 *   4. CSV data previously loaded (for comparison purposes)
 *
 * Execution Instructions:
 *   1. Verify Parquet file exists: LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN='.*\\.parquet';
 *   2. Note current row count in raw_transactions
 *   3. Execute this script in Snowflake worksheet
 *   4. Compare load performance with CSV load
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-04
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA RAW;
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

/*******************************************************************************
 * STEP 1: PRE-LOAD VALIDATION
 *
 * Verify Parquet file is present and accessible
 ******************************************************************************/

-- List all Parquet files in the stage
-- Expected: online_retail.parquet (~7-8 MB, much smaller than 91 MB CSV)
LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN = '.*\.parquet';

-- Convert LIST output to table format for easier reading
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

/*******************************************************************************
 * STEP 2: DATA PREVIEW
 *
 * Query Parquet file directly to preview structure and content.
 * Note how Parquet files preserve column names (unlike CSV's positional $1, $2).
 *
 * Parquet Advantages Demonstrated Here:
 * ------------------------------------
 * - Column names are built into the file format (self-documenting)
 * - Data types are preserved (no string-to-type conversion needed)
 * - Schema evolution is easier (add/remove columns without breaking loads)
 * - Metadata includes compression, encoding, and statistics
 ******************************************************************************/

SELECT
  'Data preview' AS step,
  $1:Invoice::VARCHAR AS invoice_no,
  $1:StockCode::VARCHAR AS stock_code,
  $1:Description::VARCHAR AS description,
  $1:Quantity::VARCHAR AS quantity,
  $1:InvoiceDate::VARCHAR AS invoice_date,
  $1:Price::VARCHAR AS unit_price,
  $1:"Customer ID"::VARCHAR AS customer_id,
  $1:Country::VARCHAR AS country
FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/online_retail.parquet
  (FILE_FORMAT => 'ECOMMERCE_DW.RAW.PARQUET_FORMAT')
LIMIT 10;

/*******************************************************************************
 * STEP 3: CLEAR EXISTING DATA (FOR TESTING/COMPARISON)
 *
 * Truncate the table to do a fresh load with Parquet data. This allows
 * us to compare load performance and file sizes between CSV and Parquet.
 *
 * In a production scenario, you might:
 * - Use separate tables for CSV vs Parquet testing
 * - Load into a staging table first, then merge
 * - Implement incremental loading instead of full truncate
 ******************************************************************************/

TRUNCATE TABLE ECOMMERCE_DW.RAW.raw_transactions;

-- Verify table is empty
SELECT COUNT(*) AS row_count_before_load
FROM ECOMMERCE_DW.RAW.raw_transactions;

/*******************************************************************************
 * STEP 4: LOAD DATA FROM PARQUET
 *
 * Parquet COPY INTO Pattern Differences from CSV:
 * ----------------------------------------------
 *
 * Column Access: $1:ColumnName::TargetType
 *   Parquet files store column names, so we reference them explicitly:
 *   - $1 represents the Parquet file object
 *   - :ColumnName accesses a specific column by name
 *   - ::VARCHAR casts to target data type
 *
 *   This is more robust than CSV's positional $1, $2... because:
 *   - Column order in file doesn't matter
 *   - Adding new columns to Parquet doesn't break existing queries
 *   - Self-documenting (column names visible in COPY command)
 *
 * FILE_FORMAT = PARQUET_FORMAT
 *   Uses Parquet-specific format defined in 03_create_file_formats.sql
 *   Automatically handles:
 *   - Parquet file structure (row groups, column chunks)
 *   - Compression (Snappy, Gzip, LZ4, etc.)
 *   - Encoding (dictionary, RLE, bit-packing)
 *   - Nested data structures (if present)
 *
 * Performance Characteristics:
 * --------------------------
 * Parquet loads typically show:
 * - 3-5x faster load times than equivalent CSV
 * - Lower network transfer (smaller file size)
 * - Better Snowflake optimization (columnar format matches Snowflake's internal storage)
 * - Reduced parsing overhead (no delimiter/quote handling needed)
 *
 * Why Parquet is Better for Data Lakes:
 * ------------------------------------
 * 1. Storage Cost: 70-90% smaller files = lower S3 costs
 * 2. Transfer Cost: Less data to transfer from S3 to Snowflake
 * 3. Processing Time: Faster loads = lower compute costs
 * 4. Query Performance: Columnar format enables predicate pushdown
 * 5. Schema Management: Built-in schema reduces parsing errors
 *
 * The Trade-off:
 * -------------
 * - CSV: Human-readable, universally supported, easy to debug
 * - Parquet: Optimized for performance, not human-readable, requires tools to inspect
 *
 * Best Practice:
 * -------------
 * - Receive CSV from source systems (compatibility)
 * - Convert to Parquet before uploading to S3 (performance)
 * - Use Parquet for all data lake storage
 * - Export to CSV only when needed for human consumption
 ******************************************************************************/

COPY INTO ECOMMERCE_DW.RAW.raw_transactions (
  invoice_no,
  stock_code,
  description,
  quantity,
  invoice_date,
  unit_price,
  customer_id,
  country,
  file_name,
  file_row_number
)
FROM (
  SELECT
    $1:Invoice::VARCHAR,       -- Named column access (Parquet advantage)
    $1:StockCode::VARCHAR,
    $1:Description::VARCHAR,
    $1:Quantity::VARCHAR,
    $1:InvoiceDate::VARCHAR,
    $1:Price::VARCHAR,
    $1:"Customer ID"::VARCHAR,
    $1:Country::VARCHAR,
    METADATA$FILENAME,            -- Same metadata capture as CSV
    METADATA$FILE_ROW_NUMBER
  FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.PARQUET_FORMAT')
PATTERN = '.*\\.parquet'
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

/*******************************************************************************
 * STEP 5: VERIFY LOAD RESULTS
 *
 * Check load statistics and compare with CSV load performance
 ******************************************************************************/

-- Query load history for Parquet load
-- Compare completion time with CSV load from previous script
SELECT
  'Load history' AS step,
  file_name,
  status,
  row_count,
  row_parsed,
  first_error_message,
  last_load_time AS completed_at,
  error_count
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'ECOMMERCE_DW.RAW.raw_transactions',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

/*******************************************************************************
 * STEP 6: DATA QUALITY CHECKS
 *
 * Verify data loaded correctly and matches CSV load expectations
 ******************************************************************************/

-- Total row count (should match CSV load: ~1,067,371 rows)
SELECT
  'Row count' AS check_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT file_name) AS distinct_files
FROM ECOMMERCE_DW.RAW.raw_transactions;

-- Check for null values in key columns
SELECT
  'Null analysis' AS check_name,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN invoice_no IS NULL THEN 1 ELSE 0 END) AS null_invoice_no,
  SUM(CASE WHEN stock_code IS NULL THEN 1 ELSE 0 END) AS null_stock_code,
  SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
  SUM(CASE WHEN invoice_date IS NULL THEN 1 ELSE 0 END) AS null_invoice_date,
  SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country
FROM ECOMMERCE_DW.RAW.raw_transactions;

-- Sample data from loaded table
SELECT
  'Sample data' AS check_name,
  *
FROM ECOMMERCE_DW.RAW.raw_transactions
LIMIT 10;

-- Verify audit columns are populated
SELECT
  'Audit columns' AS check_name,
  MIN(load_timestamp) AS earliest_load,
  MAX(load_timestamp) AS latest_load,
  COUNT(DISTINCT file_name) AS file_count,
  MIN(file_row_number) AS min_row_number,
  MAX(file_row_number) AS max_row_number
FROM ECOMMERCE_DW.RAW.raw_transactions;

-- Compare country distribution with CSV load (should be identical)
SELECT
  'Country distribution' AS check_name,
  country,
  COUNT(*) AS transaction_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ECOMMERCE_DW.RAW.raw_transactions
GROUP BY country
ORDER BY transaction_count DESC
LIMIT 10;

/*******************************************************************************
 * PARQUET LOAD SUMMARY
 *
 * Expected Results:
 * ----------------
 * - Row count: ~1,067,371 rows (same as CSV)
 * - Load time: 3-10 seconds (3-5x faster than CSV)
 * - File size: ~7-8 MB (92% smaller than CSV)
 * - Error count: Should be 0 (Parquet has built-in schema validation)
 *
 * Performance Comparison (Typical Results):
 * ----------------------------------------
 * Format    | File Size | Load Time | Transfer Time | Total Time
 * ----------|-----------|-----------|---------------|------------
 * CSV       | 91 MB     | 15-25s    | 5-10s         | 20-35s
 * Parquet   | 7-8 MB    | 5-8s      | 1-2s          | 6-10s
 * Speedup   | 11x       | 3-4x      | 5-8x          | 3-4x
 *
 * Cost Implications:
 * -----------------
 * 1. S3 Storage: 92% reduction = $0.92 saved per GB per month
 * 2. Data Transfer: 92% reduction = significant savings at scale
 * 3. Compute Time: 3-4x faster = lower warehouse costs
 * 4. Total Savings: 60-70% TCO reduction for large-scale data lakes
 *
 * When to Use Each Format:
 * -----------------------
 * CSV:
 * - Initial data receipt from source systems
 * - Human-readable exports for business users
 * - Legacy system integration
 * - Small datasets (< 10 MB) where conversion overhead isn't worth it
 *
 * Parquet:
 * - All data lake storage (S3, ADLS, GCS)
 * - Large datasets (> 100 MB)
 * - Frequent access patterns
 * - Analytics workloads
 * - Long-term data retention
 *
 * Conversion Pattern:
 * ------------------
 * Source → CSV → [Convert to Parquet] → S3 → Snowflake
 *
 * Use Python/Pandas, Spark, or AWS Glue to convert CSV to Parquet:
 * - pandas: df.to_parquet('file.parquet', compression='snappy')
 * - PySpark: df.write.parquet('file.parquet', compression='snappy')
 * - AWS Glue: Built-in format conversion jobs
 *
 * Next Steps:
 * ----------
 * 1. Run comprehensive benchmark: sql/raw/04_benchmark_csv_vs_parquet.sql
 * 2. Document findings: docs/benchmarks/csv-vs-parquet.md
 * 3. Decide on standard format for project (recommendation: Parquet)
 * 4. Proceed with data transformations: sql/staging/*.sql
 ******************************************************************************/
