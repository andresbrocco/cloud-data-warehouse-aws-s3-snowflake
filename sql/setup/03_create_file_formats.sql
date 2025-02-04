/*******************************************************************************
 * Script: 03_create_file_formats.sql
 * Purpose: Define file formats for loading data from S3 into Snowflake
 *
 * Description:
 *   File formats in Snowflake act as reusable templates that define how to
 *   parse different file types during data loading. By defining these formats
 *   once, we ensure consistent parsing rules across all COPY INTO operations,
 *   reducing errors and simplifying data ingestion workflows.
 *
 *   This script creates two file formats:
 *   1. CSV_FORMAT - For comma-separated value files
 *   2. PARQUET_FORMAT - For columnar Parquet files (more efficient storage)
 *
 * When to Use Each Format:
 *   - CSV: Initial data ingestion from source systems, human-readable exports
 *   - Parquet: Optimized storage in S3, faster loading, better compression
 *
 * Execution Instructions:
 *   1. Ensure the database and schemas exist (run 02_create_database_schemas.sql first)
 *   2. Connect to Snowflake with appropriate role (SYSADMIN recommended)
 *   3. Execute this entire script in a Snowflake worksheet
 *   4. Verify creation with: SHOW FILE FORMATS IN SCHEMA ECOMMERCE_DW.RAW;
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-02
 * Version: 1.0
 ******************************************************************************/

-- Set context to the appropriate database and schema
USE DATABASE ECOMMERCE_DW;
USE SCHEMA RAW;
USE ROLE SYSADMIN;

/*******************************************************************************
 * CSV FILE FORMAT
 *
 * Configuration Rationale:
 * -----------------------
 * FIELD_DELIMITER = ','
 *   Standard CSV delimiter. Our source data uses comma-separated values.
 *
 * SKIP_HEADER = 1
 *   The first row contains column names, not data. Skipping it prevents
 *   attempting to load header text into typed columns.
 *
 * FIELD_OPTIONALLY_ENCLOSED_BY = '"'
 *   Fields may be wrapped in double quotes, especially if they contain
 *   commas or special characters (e.g., product descriptions).
 *   This ensures proper parsing of quoted strings.
 *
 * NULL_IF = ('NULL', 'null', '')
 *   Defines which string values should be interpreted as SQL NULL.
 *   This handles inconsistent null representation in source data:
 *   - Explicit 'NULL' or 'null' text
 *   - Empty strings (which should be NULL for non-string columns)
 *
 * EMPTY_FIELD_AS_NULL = TRUE
 *   Treats completely empty fields (nothing between delimiters: ,,) as NULL.
 *   This is crucial for handling missing data in CSV exports where fields
 *   are simply omitted rather than explicitly marked as NULL.
 *
 * Why These Settings Matter:
 * -------------------------
 * CSV files from different source systems can have subtle format variations.
 * These settings handle the most common inconsistencies:
 * - Quoted vs. unquoted fields
 * - Missing vs. empty vs. NULL values
 * - Headers vs. data-only files
 *
 * This format definition ensures consistent, predictable parsing behavior
 * across all data loads, reducing the "it works in dev but fails in prod"
 * scenarios common with ad-hoc COPY commands.
 ******************************************************************************/

CREATE OR REPLACE FILE FORMAT ECOMMERCE_DW.RAW.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  COMPRESSION = 'AUTO'
  COMMENT = 'CSV format for e-commerce source data with standard delimiters and null handling';

-- Confirm CSV format creation
SELECT 'CSV_FORMAT created successfully' AS status;

/*******************************************************************************
 * PARQUET FILE FORMAT
 *
 * Configuration Rationale:
 * -----------------------
 * TYPE = 'PARQUET'
 *   Parquet is a columnar storage format that provides:
 *   - Superior compression (typically 70-80% smaller than CSV)
 *   - Faster load times (Snowflake can read columns in parallel)
 *   - Built-in schema (column names and types are stored in file metadata)
 *   - Better for large-scale data lake scenarios
 *
 * Why Parquet?
 * -----------
 * While our initial data may arrive as CSV, converting to Parquet for
 * S3 storage provides significant benefits:
 *
 * 1. Cost Savings: Smaller files = lower S3 storage costs
 * 2. Performance: Columnar format enables faster queries and loads
 * 3. Schema Evolution: Metadata enables easier schema validation
 * 4. Industry Standard: Widely used in modern data lake architectures
 *
 * Usage Pattern:
 * -------------
 * 1. Receive CSV from source systems
 * 2. Convert to Parquet using Python/Pandas or Spark
 * 3. Upload Parquet to S3
 * 4. Load into Snowflake using this format definition
 * 5. Enjoy faster loads and lower storage costs
 *
 * Note on Compression:
 * -------------------
 * Parquet files are typically already compressed (Snappy, Gzip, etc.).
 * Snowflake automatically detects and handles the compression, so we
 * don't need to specify compression settings for Parquet like we do for CSV.
 ******************************************************************************/

CREATE OR REPLACE FILE FORMAT ECOMMERCE_DW.RAW.PARQUET_FORMAT
  TYPE = 'PARQUET'
  COMPRESSION = 'AUTO'
  COMMENT = 'Parquet format for efficient columnar storage and optimized loading from S3';

-- Confirm Parquet format creation
SELECT 'PARQUET_FORMAT created successfully' AS status;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- List all file formats in the RAW schema
SHOW FILE FORMATS IN SCHEMA ECOMMERCE_DW.RAW;

-- Display detailed properties of CSV format
DESC FILE FORMAT ECOMMERCE_DW.RAW.CSV_FORMAT;

-- Display detailed properties of Parquet format
DESC FILE FORMAT ECOMMERCE_DW.RAW.PARQUET_FORMAT;

/*******************************************************************************
 * USAGE EXAMPLES
 *
 * How to Reference These Formats in COPY Commands:
 * ------------------------------------------------
 *
 * -- Loading CSV files:
 * COPY INTO ECOMMERCE_DW.RAW.transactions
 * FROM @my_s3_stage/data/transactions.csv
 * FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.CSV_FORMAT');
 *
 * -- Loading Parquet files:
 * COPY INTO ECOMMERCE_DW.RAW.transactions
 * FROM @my_s3_stage/data/transactions.parquet
 * FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.PARQUET_FORMAT');
 *
 * -- Pattern matching for multiple files:
 * COPY INTO ECOMMERCE_DW.RAW.transactions
 * FROM @my_s3_stage/data/
 * FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.CSV_FORMAT')
 * PATTERN = '.*transactions.*\\.csv';
 *
 * Benefits of Named Formats:
 * -------------------------
 * 1. Consistency: Same parsing rules across all loads
 * 2. Maintainability: Update format once, affects all loads
 * 3. Readability: COPY commands are simpler and clearer
 * 4. Reusability: Share formats across multiple tables and scripts
 *
 * Next Steps:
 * ----------
 * 1. Create external stages pointing to S3 buckets
 * 2. Define RAW layer tables matching source data structure
 * 3. Use these formats in COPY INTO commands for data ingestion
 ******************************************************************************/
