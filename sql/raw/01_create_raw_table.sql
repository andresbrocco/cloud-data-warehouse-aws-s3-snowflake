/*******************************************************************************
 * Script: 01_create_raw_table.sql
 * Purpose: Create raw layer table for e-commerce transaction data
 *
 * Description:
 *   This script creates the raw_transactions table in the RAW_LAYER schema.
 *   The RAW layer follows the "bronze" layer pattern in a medallion architecture,
 *   storing data exactly as received from source systems without transformations.
 *
 *   Key principles for RAW layer tables:
 *   - Store data as close to source format as possible
 *   - Use permissive data types (VARCHAR instead of strict types)
 *   - Minimal validation - accept data "as-is"
 *   - Include audit columns for data lineage tracking
 *   - Immutable - once loaded, raw data should not be modified
 *
 * Data Source:
 *   UCI Online Retail II dataset from Kaggle
 *   Source: https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci
 *   Contains: UK e-commerce transactions from 2009-2011
 *   Size: ~1M rows, 8 columns
 *
 * Schema Design:
 *   All business columns use VARCHAR to accept any source data format.
 *   Type validation and conversion happens downstream in STAGING layer.
 *   This approach prevents load failures due to data quality issues in source.
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. File formats defined (sql/setup/03_create_file_formats.sql)
 *   3. External stage created (sql/setup/04_create_external_stage.sql)
 *
 * Execution Instructions:
 *   1. Ensure prerequisites are completed
 *   2. Execute this entire script in Snowflake worksheet
 *   3. Verify table creation: DESC TABLE ECOMMERCE_DW.RAW.raw_transactions;
 *   4. Proceed to data loading scripts
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-04
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA RAW;
USE ROLE SYSADMIN;

/*******************************************************************************
 * RAW TRANSACTIONS TABLE
 *
 * Column Definitions and Rationale:
 * --------------------------------
 *
 * invoice_no (VARCHAR(50))
 *   - Transaction/order identifier from source system
 *   - VARCHAR to handle alphanumeric codes (e.g., "C12345" for cancellations)
 *   - Size 50 accommodates typical invoice number formats with room for growth
 *   - Later analysis will use this to identify order cancellations (prefix 'C')
 *
 * stock_code (VARCHAR(50))
 *   - Product SKU/identifier
 *   - VARCHAR because codes may include letters (e.g., "GIFT001", "POST")
 *   - Source system may use various formats (numeric, alphanumeric, special codes)
 *   - Size 50 handles extended SKU formats with prefixes/suffixes
 *
 * description (VARCHAR(500))
 *   - Product description/name
 *   - Size 500 accommodates detailed product descriptions
 *   - May contain special characters, punctuation, multi-word descriptions
 *   - Nullable because some products may lack descriptions in source
 *
 * quantity (VARCHAR(50))
 *   - Quantity ordered/returned
 *   - VARCHAR in RAW layer to accept any input format (even invalid data)
 *   - Will convert to INTEGER in STAGING after validation
 *   - Negative quantities indicate returns/cancellations
 *   - Size 50 is generous for numeric values but allows for data quality issues
 *
 * invoice_date (VARCHAR(50))
 *   - Transaction timestamp from source system
 *   - VARCHAR to accept various date/time formats from source
 *   - Will parse and convert to TIMESTAMP in STAGING layer
 *   - Size 50 handles various datetime formats (ISO 8601, US format, etc.)
 *   - Source may include timezone information or fractional seconds
 *
 * unit_price (VARCHAR(50))
 *   - Price per unit in source currency
 *   - VARCHAR to handle different decimal formats (1.23, 1,23, etc.)
 *   - Will convert to DECIMAL in STAGING after validation
 *   - Size 50 handles currency values with high precision
 *
 * customer_id (VARCHAR(50))
 *   - Customer identifier from source system
 *   - Nullable because source data contains transactions without customer IDs
 *   - These represent guest checkouts or data quality issues
 *   - VARCHAR accommodates numeric or alphanumeric customer codes
 *
 * country (VARCHAR(100))
 *   - Customer's country (transaction location)
 *   - Size 100 handles full country names in various formats
 *   - Later standardization will map to ISO country codes in STAGING
 *   - May contain spelling variations that need cleaning
 *
 * Audit Columns (Metadata for Data Lineage):
 * -----------------------------------------
 *
 * load_timestamp (TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
 *   - When this row was loaded into Snowflake
 *   - NTZ (No Time Zone) because we want to track Snowflake's clock, not source time
 *   - Useful for troubleshooting load issues and data freshness tracking
 *   - DEFAULT ensures every row gets timestamped automatically
 *
 * file_name (VARCHAR(500))
 *   - Source file name (populated during COPY INTO)
 *   - Enables tracing data back to specific source files
 *   - Critical for data lineage and troubleshooting
 *   - Size 500 accommodates long S3 paths and file names
 *
 * file_row_number (NUMBER)
 *   - Row number within source file (populated during COPY INTO)
 *   - Combined with file_name, provides exact source location for each row
 *   - Essential for investigating data quality issues
 *   - Enables re-loading specific problematic rows if needed
 *
 * Why Use VARCHAR for Everything in RAW Layer?
 * -------------------------------------------
 * 1. Prevents load failures: Bad data doesn't break the pipeline
 * 2. Preserves source data exactly: No implicit conversions or data loss
 * 3. Enables data quality analysis: Can identify and count invalid values
 * 4. Flexible for schema changes: Source system changes don't break loads
 * 5. Audit trail: Shows exactly what was received before any transformations
 *
 * The STAGING layer will handle proper typing, validation, and cleaning.
 * This separation of concerns is a key data engineering best practice.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.RAW.raw_transactions (
  -- Business columns from source system (all VARCHAR for maximum compatibility)
  invoice_no          VARCHAR(50),
  stock_code          VARCHAR(50),
  description         VARCHAR(500),
  quantity            VARCHAR(50),
  invoice_date        VARCHAR(50),
  unit_price          VARCHAR(50),
  customer_id         VARCHAR(50),
  country             VARCHAR(100),

  -- Audit/metadata columns for data lineage and troubleshooting
  load_timestamp      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  file_name           VARCHAR(500),
  file_row_number     NUMBER
)
COMMENT = 'Raw e-commerce transactions loaded from S3 without transformation. Bronze layer - immutable source of truth.';

-- Confirm table creation
SELECT 'raw_transactions table created successfully' AS status;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.RAW.raw_transactions;

-- Display table metadata
SHOW TABLES LIKE 'raw_transactions' IN SCHEMA ECOMMERCE_DW.RAW;

-- Verify table is empty (before data load)
SELECT COUNT(*) AS row_count
FROM ECOMMERCE_DW.RAW.raw_transactions;

/*******************************************************************************
 * TABLE DESIGN NOTES
 *
 * Why Not Use Constraints in RAW Layer?
 * ------------------------------------
 * Notice this table has NO:
 *   - Primary keys
 *   - Foreign keys
 *   - NOT NULL constraints (except audit columns)
 *   - CHECK constraints
 *   - Unique constraints
 *
 * This is intentional. The RAW layer is designed to accept ALL source data,
 * even if it violates business rules. Data quality enforcement happens in
 * STAGING and PRODUCTION layers where:
 *   - Invalid rows can be logged to error tables
 *   - Business rules are explicitly documented and enforced
 *   - Data quality metrics can be calculated
 *   - Source data remains unchanged for reprocessing
 *
 * Snowflake-Specific Features Used:
 * --------------------------------
 * - TIMESTAMP_NTZ: Snowflake's timezone-naive timestamp type
 * - DEFAULT CURRENT_TIMESTAMP(): Automatic timestamping
 * - COMMENT: Table-level documentation (visible in data catalog)
 * - CREATE OR REPLACE: Idempotent script execution
 *
 * Storage Considerations:
 * ---------------------
 * - Snowflake automatically compresses VARCHAR columns
 * - Micro-partitioning handles clustering automatically
 * - No indexes needed - Snowflake uses metadata-based pruning
 * - Time Travel enabled by default (allows querying historical data)
 *
 * Performance Considerations:
 * -------------------------
 * - VARCHAR columns don't significantly impact query performance
 * - Type conversion happens at query time (small overhead)
 * - STAGING layer will use proper types for optimal performance
 * - This table is primarily for landing data, not for analytics
 *
 * Data Governance:
 * ---------------
 * - This table serves as immutable source of truth
 * - Never UPDATE or DELETE from RAW layer in production
 * - Reprocess STAGING/PRODUCTION layers from RAW if logic changes
 * - Enables full data lineage tracking from source to analytics
 *
 * Next Steps:
 * ----------
 * 1. Load CSV data: sql/raw/02_load_data_csv.sql
 * 2. Load Parquet data: sql/raw/03_load_data_parquet.sql
 * 3. Compare performance: sql/raw/04_benchmark_csv_vs_parquet.sql
 * 4. Transform to STAGING: sql/staging/*.sql (future implementation)
 ******************************************************************************/
