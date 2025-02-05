/*******************************************************************************
 * Script: 01_create_staging_table.sql
 * Purpose: Create staging layer table with cleaned and validated data structure
 *
 * Description:
 *   This script creates the stg_orders table in the STAGING_LAYER schema.
 *   The STAGING layer follows the "silver" layer pattern in a medallion architecture,
 *   transforming raw data into business-ready format with proper types, validation,
 *   and quality checks.
 *
 *   Key principles for STAGING layer tables:
 *   - Convert VARCHAR to appropriate business data types (INTEGER, DECIMAL, TIMESTAMP)
 *   - Add computed columns derived from source data
 *   - Include data quality flags to track validation results
 *   - Maintain data lineage back to RAW layer
 *   - Prepare data structure for downstream dimensional modeling
 *
 * Data Flow:
 *   RAW.raw_transactions (VARCHAR columns) → STAGING.stg_orders (typed columns)
 *
 * Schema Design Philosophy:
 *   This table bridges the gap between raw data and analytical models:
 *   1. Business Keys: Identifiers preserved from source for joining and tracking
 *   2. Typed Columns: Proper data types enable calculations and optimizations
 *   3. Computed Fields: Pre-calculate business metrics (e.g., total_amount)
 *   4. Quality Flags: Track which records passed validation rules
 *   5. Metadata: Lineage tracking for debugging and audit trails
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. RAW layer table exists (sql/raw/01_create_raw_table.sql)
 *   3. RAW layer loaded with data (sql/raw/03_load_data_parquet.sql)
 *
 * Execution Instructions:
 *   1. Ensure prerequisites are completed
 *   2. Execute this entire script in Snowflake worksheet
 *   3. Verify table creation: DESC TABLE ECOMMERCE_DW.STAGING.stg_orders;
 *   4. Proceed to data loading script (02_load_staging_from_raw.sql)
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
 * STAGING ORDERS TABLE
 *
 * Column Definitions and Design Rationale:
 * ----------------------------------------
 *
 * BUSINESS KEYS (Identifiers from Source System)
 * ----------------------------------------------
 *
 * invoice_no (VARCHAR(50))
 *   - Order/transaction identifier preserved from source
 *   - Kept as VARCHAR because format includes letters (e.g., "C12345" for cancellations)
 *   - Used to group line items into orders in PRODUCTION layer
 *   - Critical for tracking order cancellations and returns
 *
 * stock_code (VARCHAR(50))
 *   - Product SKU preserved from source
 *   - Kept as VARCHAR to handle alphanumeric codes (e.g., "POST", "GIFT001")
 *   - Maps to product dimension in PRODUCTION layer
 *   - Foundation for product analytics and inventory tracking
 *
 * DESCRIPTIVE ATTRIBUTES (Business Metadata)
 * ------------------------------------------
 *
 * description (VARCHAR(500))
 *   - Product name/description from source
 *   - Used for product categorization and reporting
 *   - May be cleansed (trimmed, standardized) from RAW layer
 *   - Nullable because some products legitimately lack descriptions
 *
 * country (VARCHAR(100))
 *   - Customer's country for geographic analysis
 *   - Will be standardized and mapped to country dimension
 *   - Foundation for regional sales reporting
 *   - May need cleansing for spelling variations and standardization
 *
 * MEASURES (Numeric Business Data)
 * --------------------------------
 *
 * quantity (INTEGER)
 *   - Number of units ordered/returned
 *   - Converted from VARCHAR in RAW layer using TRY_CAST for safety
 *   - Negative values indicate returns or cancellations
 *   - Zero values are flagged as data quality issues
 *   - Used for inventory calculations and order metrics
 *
 * unit_price (DECIMAL(10,2))
 *   - Price per unit in GBP (source currency)
 *   - DECIMAL(10,2) handles up to 99,999,999.99 with 2 decimal precision
 *   - Converted from VARCHAR using TRY_CAST to handle invalid values gracefully
 *   - Negative or zero prices are flagged as data quality issues
 *   - Foundation for revenue calculations
 *
 * total_amount (DECIMAL(12,2))
 *   - Computed field: quantity * unit_price
 *   - Pre-calculated to avoid repeated computation in queries
 *   - DECIMAL(12,2) handles larger order totals
 *   - Negative values indicate returns/refunds
 *   - Core measure for revenue analysis and aggregations
 *
 * TEMPORAL COLUMNS (Time-Based Analysis)
 * --------------------------------------
 *
 * invoice_date (TIMESTAMP_NTZ)
 *   - Transaction timestamp converted from source
 *   - TIMESTAMP_NTZ (no timezone) for consistent date-based analysis
 *   - Used for time-series analysis, trending, and seasonality studies
 *   - NULL values indicate data quality issues in source
 *   - Foundation for temporal slicing in reports
 *
 * invoice_date_key (INTEGER)
 *   - Surrogate key in YYYYMMDD format for date dimension joins
 *   - Enables efficient joining with dim_date in PRODUCTION layer
 *   - Example: 2010-12-01 → 20101201
 *   - Improves query performance vs. date joins
 *   - Standard pattern for dimensional modeling
 *
 * CUSTOMER DIMENSION
 * -----------------
 *
 * customer_id (INTEGER)
 *   - Customer identifier converted from source
 *   - Nullable because source contains guest transactions
 *   - NULL values represent guest checkouts or missing data
 *   - Used to join with customer dimension in PRODUCTION layer
 *   - Foundation for customer behavior and retention analysis
 *
 * DATA QUALITY FLAGS (Validation Tracking)
 * ----------------------------------------
 *
 * is_valid (BOOLEAN DEFAULT TRUE)
 *   - Master flag indicating if record passed all validation rules
 *   - TRUE: Clean record ready for PRODUCTION layer
 *   - FALSE: Issues detected, see quality_issues for details
 *   - Enables filtering valid records without complex WHERE clauses
 *   - Allows retaining invalid records for quality monitoring
 *
 * quality_issues (VARCHAR(500))
 *   - Pipe-delimited list of validation failures
 *   - Example: "Negative quantity | Invalid unit price | Missing invoice date"
 *   - NULL if no issues detected (is_valid = TRUE)
 *   - Enables data quality reporting and root cause analysis
 *   - Critical for continuous data quality improvement
 *
 * METADATA COLUMNS (Data Lineage and Audit)
 * -----------------------------------------
 *
 * _processed_at (TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
 *   - When this row was processed from RAW to STAGING
 *   - Different from load_timestamp in RAW (which tracks source arrival)
 *   - Enables tracking transformation pipeline execution times
 *   - Useful for incremental processing patterns
 *   - Underscore prefix denotes internal metadata column
 *
 * _source_row_id (NUMBER)
 *   - Reference to row number in RAW layer source file
 *   - Enables tracing back to exact source record
 *   - Critical for investigating data quality issues
 *   - Combined with file metadata from RAW provides full lineage
 *   - Underscore prefix denotes internal metadata column
 *
 * Why Use Proper Types in STAGING Layer?
 * --------------------------------------
 * 1. Type Safety: Catches data quality issues during transformation
 * 2. Performance: Numeric types are more efficient than VARCHAR for calculations
 * 3. Storage: Proper types reduce storage footprint (compressed better)
 * 4. Query Optimization: Snowflake can optimize queries with known types
 * 5. Business Semantics: Types convey meaning (DECIMAL for money, INTEGER for counts)
 *
 * The STAGING layer acts as a quality gate between raw ingestion and analytics.
 * Invalid records are flagged but retained for quality monitoring and debugging.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.STAGING.stg_orders (
  -- Business keys (identifiers preserved from source)
  invoice_no              VARCHAR(50),
  stock_code              VARCHAR(50),

  -- Descriptive attributes (business metadata)
  description             VARCHAR(500),
  country                 VARCHAR(100),

  -- Measures (numeric business data with proper types)
  quantity                INTEGER,
  unit_price              DECIMAL(10,2),
  total_amount            DECIMAL(12,2),  -- Computed: quantity * unit_price

  -- Temporal columns (time-based analysis)
  invoice_date            TIMESTAMP_NTZ,
  invoice_date_key        INTEGER,        -- YYYYMMDD format for dim_date join

  -- Customer dimension
  customer_id             INTEGER,

  -- Data quality flags (validation tracking)
  is_valid                BOOLEAN DEFAULT TRUE,
  quality_issues          VARCHAR(500),

  -- Metadata columns (data lineage and audit trail)
  _processed_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _source_row_id          NUMBER
)
COMMENT = 'Staging: cleaned and validated e-commerce orders with proper data types and quality flags';

-- Confirm table creation
SELECT 'stg_orders table created successfully' AS status;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.STAGING.stg_orders;

-- Display table metadata
SHOW TABLES LIKE 'stg_orders' IN SCHEMA ECOMMERCE_DW.STAGING;

-- Verify table is empty (before data load)
SELECT COUNT(*) AS row_count
FROM ECOMMERCE_DW.STAGING.stg_orders;

/*******************************************************************************
 * TABLE DESIGN NOTES
 *
 * Data Quality Strategy in STAGING Layer:
 * ---------------------------------------
 * This table uses a "flag but don't filter" approach to data quality:
 *
 * 1. ALL records from RAW are attempted for transformation
 * 2. Invalid records are flagged (is_valid = FALSE) with reasons
 * 3. Invalid records are RETAINED for quality monitoring
 * 4. PRODUCTION layer only consumes valid records (WHERE is_valid = TRUE)
 *
 * Benefits of this approach:
 * - Enables data quality trend analysis over time
 * - Facilitates root cause investigation of quality issues
 * - Prevents silent data loss (bad records are visible)
 * - Supports data quality SLA monitoring
 *
 * Validation Rules Applied in 02_load_staging_from_raw.sql:
 * ---------------------------------------------------------
 * - Negative or zero quantity → Invalid
 * - Negative or zero unit_price → Invalid
 * - NULL or unparseable invoice_date → Invalid
 * - NULL invoice_no → Invalid
 * - Invoice numbers starting with 'C' → Cancelled order, marked invalid
 *
 * Computed Columns Strategy:
 * -------------------------
 * total_amount is pre-computed rather than calculated at query time because:
 * - Reduces computation cost in analytics queries
 * - Ensures consistency across all reports
 * - Handles edge cases once (NULL handling, precision)
 * - Improves query performance for aggregations
 *
 * Snowflake-Specific Features Used:
 * --------------------------------
 * - TIMESTAMP_NTZ: Timezone-naive timestamps for consistent date logic
 * - BOOLEAN: Native boolean type for is_valid flag
 * - DEFAULT values: Automatic population of metadata columns
 * - COMMENT: Table-level documentation for data catalog
 * - CREATE OR REPLACE: Idempotent script execution
 *
 * Performance Considerations:
 * -------------------------
 * - Proper data types enable better compression (smaller storage)
 * - INTEGER and DECIMAL types optimize aggregation performance
 * - invoice_date_key enables efficient date dimension joins
 * - No explicit indexes needed (Snowflake uses metadata-based pruning)
 * - Table will auto-cluster based on access patterns
 *
 * Why Not Use Constraints?
 * -----------------------
 * Notice this table has NO:
 * - Primary keys
 * - Foreign keys
 * - NOT NULL constraints (except defaults)
 * - CHECK constraints
 *
 * Rationale:
 * - STAGING is a transformation layer, not an analytical layer
 * - Invalid data is flagged but not rejected (quality monitoring)
 * - Constraints would prevent loading records with quality issues
 * - PRODUCTION layer will enforce referential integrity
 *
 * Data Lineage Chain:
 * ------------------
 * Source CSV → S3 → RAW.raw_transactions → STAGING.stg_orders → PRODUCTION.fact_sales
 *
 * Each layer adds value:
 * - RAW: Immutable source of truth
 * - STAGING: Typed, validated, business-ready
 * - PRODUCTION: Dimensional model optimized for analytics
 *
 * Next Steps:
 * ----------
 * 1. Load and transform data: sql/staging/02_load_staging_from_raw.sql
 * 2. Run quality checks: sql/staging/03_staging_quality_checks.sql
 * 3. Build PRODUCTION layer: sql/production/*.sql (future implementation)
 ******************************************************************************/
