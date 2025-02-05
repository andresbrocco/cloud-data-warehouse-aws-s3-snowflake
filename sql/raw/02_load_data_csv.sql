/*******************************************************************************
 * Script: 02_load_data_csv.sql
 * Purpose: Load e-commerce transaction data from CSV files in S3
 *
 * Description:
 *   This script demonstrates the COPY INTO command pattern for loading CSV
 *   data from S3 into Snowflake. It includes proper error handling, validation,
 *   and verification steps that are essential for production data pipelines.
 *
 *   The script follows a safe loading pattern:
 *   1. Truncate existing data (for repeatable testing)
 *   2. Preview data before loading (validation)
 *   3. Execute COPY INTO with metadata capture
 *   4. Verify load results and data quality
 *   5. Review any errors or rejected rows
 *
 * Loading Strategy:
 *   - Uses external stage created in previous step
 *   - Leverages CSV_FORMAT defined earlier for consistent parsing
 *   - Captures file name and row number for data lineage
 *   - Uses ON_ERROR = 'CONTINUE' to load valid rows despite some errors
 *
 * Prerequisites:
 *   1. External stage configured (sql/setup/04_create_external_stage.sql)
 *   2. Raw table created (sql/raw/01_create_raw_table.sql)
 *   3. CSV file uploaded to S3 bucket (online_retail.csv)
 *
 * Execution Instructions:
 *   1. Verify CSV file exists: LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN='.*\\.csv';
 *   2. Execute this script in Snowflake worksheet
 *   3. Review load statistics and error counts
 *   4. Validate data quality with sample queries
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
 * Before loading data, verify that:
 * - The stage exists and is accessible
 * - CSV files are present in S3
 * - File sizes and counts match expectations
 ******************************************************************************/

-- List all CSV files in the stage
-- This should show online_retail.csv with size around 91 MB
LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN = '.*\.csv';

-- Convert LIST output to table format for easier reading
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

/*******************************************************************************
 * STEP 2: DATA PREVIEW
 *
 * Query the stage directly to preview data before loading. This helps:
 * - Verify file format is correctly configured
 * - Check column alignment matches table structure
 * - Identify potential data quality issues early
 * - Estimate load time based on file size
 *
 * The SELECT FROM @stage pattern reads files without loading them into
 * a table. It's a powerful Snowflake feature for data exploration and
 * validation before committing to a full load.
 ******************************************************************************/

SELECT
  'Data preview' AS step,
  $1 AS invoice_no,
  $2 AS stock_code,
  $3 AS description,
  $4 AS quantity,
  $5 AS invoice_date,
  $6 AS unit_price,
  $7 AS customer_id,
  $8 AS country
FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/online_retail.csv
  (FILE_FORMAT => 'ECOMMERCE_DW.RAW.CSV_FORMAT')
LIMIT 10;

/*******************************************************************************
 * STEP 3: CLEAR EXISTING DATA (FOR TESTING/REPROCESSING)
 *
 * TRUNCATE removes all rows while preserving table structure. Use this
 * when reloading data or testing the load process. In production, you
 * might want to:
 * - Load to a temporary staging table first
 * - Swap tables after validation (CREATE OR REPLACE TABLE ... AS SELECT)
 * - Use incremental loading patterns instead of full truncate/reload
 *
 * IMPORTANT: Comment out this line in production if you need to preserve
 * existing data or implement incremental loading.
 ******************************************************************************/

TRUNCATE TABLE ECOMMERCE_DW.RAW.raw_transactions;

-- Verify table is empty
SELECT COUNT(*) AS row_count_before_load
FROM ECOMMERCE_DW.RAW.raw_transactions;

/*******************************************************************************
 * STEP 4: LOAD DATA FROM CSV
 *
 * COPY INTO Command Breakdown:
 * ---------------------------
 *
 * COPY INTO raw_transactions
 *   Target table for data load
 *
 * FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/online_retail.csv
 *   Source: external stage + specific file name
 *   Stage reference (@ prefix) uses configuration from 04_create_external_stage.sql
 *
 * FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
 *   Uses CSV_FORMAT defined in 03_create_file_formats.sql
 *   Explicit reference ensures correct parsing rules (delimiter, quotes, nulls)
 *   Could be omitted since CSV_FORMAT is the stage default, but explicit is clearer
 *
 * PATTERN = '.*\\.csv'
 *   Regular expression to match files
 *   Useful when loading multiple files or in automated pipelines
 *   Here it matches any file ending in .csv
 *   In this case, we have only one file, but pattern shows best practice
 *
 * Column Mapping with $1, $2, etc:
 * --------------------------------
 * $1, $2... represent columns in the CSV file (positional references)
 * This explicit mapping:
 *   - Makes the load process self-documenting
 *   - Allows reordering columns between file and table
 *   - Enables transformations during load (e.g., UPPER($1))
 *   - Protects against CSV column order changes
 *
 * Metadata Columns:
 * ----------------
 * METADATA$FILENAME
 *   Snowflake pseudo-column containing source file name
 *   Critical for data lineage - trace each row back to source file
 *
 * METADATA$FILE_ROW_NUMBER
 *   Row number within the source file
 *   Combined with file name, provides exact location of each row in source
 *   Essential for troubleshooting data quality issues
 *
 * Error Handling:
 * --------------
 * ON_ERROR = 'CONTINUE'
 *   Continue loading even if some rows have errors
 *   Alternative options:
 *     - ABORT_STATEMENT: Stop on first error (default)
 *     - SKIP_FILE: Skip entire file if any errors
 *     - SKIP_FILE_N: Skip file if more than N errors
 *     - SKIP_FILE_N%: Skip file if more than N% rows have errors
 *
 *   CONTINUE is useful when:
 *   - You want to load all valid rows
 *   - Invalid rows should be analyzed separately
 *   - Partial data is acceptable in RAW layer
 *
 * RETURN_FAILED_ONLY = TRUE
 *   Show only rows that failed to load (not successful rows)
 *   Helps focus on data quality issues
 *   Successful rows don't clutter the result set
 *
 * Why This Approach?
 * -----------------
 * This pattern balances:
 * - Data lineage (file tracking)
 * - Error tolerance (continues on errors)
 * - Debugging capability (returns failed rows)
 * - Production readiness (handles imperfect data gracefully)
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
    $1,                          -- invoice_no
    $2,                          -- stock_code
    $3,                          -- description
    $4,                          -- quantity
    $5,                          -- invoice_date
    $6,                          -- unit_price
    $7,                          -- customer_id
    $8,                          -- country
    METADATA$FILENAME,           -- Capture source file name for lineage
    METADATA$FILE_ROW_NUMBER     -- Capture row number in source file
  FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.CSV_FORMAT')
PATTERN = '.*\\.csv'
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

/*******************************************************************************
 * STEP 5: VERIFY LOAD RESULTS
 *
 * Check load statistics from the COPY INTO operation:
 * - How many rows were processed?
 * - How many loaded successfully?
 * - Were there any errors?
 * - How long did the load take?
 ******************************************************************************/

-- Query load history for this table
-- Shows detailed statistics about the most recent COPY operations
SELECT
  'Load history' AS step,
  file_name,
  status,
  row_count,
  row_parsed,
  first_error_message,
  first_error_line_number,
  first_error_character_pos,
  first_error_column_name,
  last_load_time,
  error_count,
  error_limit
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'ECOMMERCE_DW.RAW.raw_transactions',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

/*******************************************************************************
 * STEP 6: DATA QUALITY CHECKS
 *
 * Validate the loaded data to ensure quality and completeness:
 * - Row counts match expectations (~1M rows)
 * - Key columns are populated
 * - Data distribution looks reasonable
 * - File metadata was captured correctly
 ******************************************************************************/

-- Total row count
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

-- Distribution by country (top 10)
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
 * LOAD SUMMARY AND NEXT STEPS
 *
 * At this point, you should have:
 * ✓ CSV data loaded into raw_transactions table
 * ✓ ~1M rows (actual count: ~1,067,371 rows based on source dataset)
 * ✓ Audit columns populated with file tracking information
 * ✓ Load statistics reviewed for errors
 *
 * Expected Results:
 * ----------------
 * - Row count: ~1,067,371 rows (full Online Retail II dataset)
 * - Load time: 10-30 seconds (varies by warehouse size and network)
 * - File size: ~91 MB compressed CSV
 * - Error count: Should be 0 or very low (< 0.1%)
 *
 * Common Issues and Solutions:
 * ---------------------------
 * 1. "File not found" error
 *    → Verify file uploaded to S3: LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE;
 *    → Check file name spelling matches exactly
 *
 * 2. "Access Denied" error
 *    → Verify storage integration: DESC INTEGRATION S3_INTEGRATION;
 *    → Check IAM role permissions in AWS
 *
 * 3. "Number of columns in file does not match" error
 *    → Verify CSV_FORMAT settings: DESC FILE FORMAT CSV_FORMAT;
 *    → Check SKIP_HEADER is set to 1
 *    → Ensure file has 8 columns
 *
 * 4. High error count (> 1%)
 *    → Review first_error_message in COPY_HISTORY
 *    → Check CSV_FORMAT null handling settings
 *    → Investigate failed rows with VALIDATION_MODE = RETURN_ERRORS
 *
 * Next Steps:
 * ----------
 * 1. Load Parquet version: sql/raw/03_load_data_parquet.sql
 * 2. Compare CSV vs Parquet performance: sql/raw/04_benchmark_csv_vs_parquet.sql
 * 3. Transform data to STAGING layer: sql/staging/*.sql (future)
 * 4. Build dimensional model: sql/production/*.sql (future)
 ******************************************************************************/
