/*******************************************************************************
 * Script: 02_create_database_schemas.sql
 * Purpose: Create the ECOMMERCE_DW database and multi-layer schema architecture
 *
 * Description:
 *   This script establishes the foundational database structure for our cloud
 *   data warehouse. It implements a three-layer architecture (RAW, STAGING,
 *   PRODUCTION) based on the medallion pattern, which provides clear separation
 *   of concerns and enables flexible data transformation workflows.
 *
 * Architecture Pattern:
 *   - RAW Layer (Bronze): Preserves source data exactly as received from S3
 *   - STAGING Layer (Silver): Contains cleaned, validated, and standardized data
 *   - PRODUCTION Layer (Gold): Hosts business-ready dimensional model for analytics
 *
 * Execution Instructions:
 *   1. Connect to your Snowflake account with ACCOUNTADMIN or SYSADMIN role
 *   2. Execute this entire script in a Snowflake worksheet
 *   3. Verify creation by running: SHOW DATABASES; SHOW SCHEMAS IN ECOMMERCE_DW;
 *
 * Design Principles:
 *   - Idempotency: Uses IF NOT EXISTS to allow safe re-execution
 *   - Documentation: Each object includes COMMENT for self-documentation
 *   - Immutability: RAW layer preserves original data without modifications
 *   - Traceability: Each layer enables audit trail and reprocessing capability
 *   - Separation: Clear boundaries between ingestion, transformation, and analytics
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-02
 * Version: 1.0
 ******************************************************************************/

-- Set the context to ensure we're working with the correct role
-- Note: You may need SYSADMIN or ACCOUNTADMIN privileges for database creation
USE ROLE SYSADMIN;

/*******************************************************************************
 * DATABASE CREATION
 ******************************************************************************/

-- Create the main data warehouse database
-- This will serve as the container for all our schemas and data objects
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW
  COMMENT = 'E-commerce data warehouse with multi-layer architecture following medallion pattern (Bronze/Silver/Gold)';

-- Confirm database creation
SELECT 'Database ECOMMERCE_DW created successfully' AS status;

/*******************************************************************************
 * SCHEMA CREATION - Multi-Layer Architecture
 ******************************************************************************/

-- RAW LAYER (Bronze)
-- Purpose: Store unprocessed data exactly as received from source systems
-- Characteristics:
--   - No transformations applied
--   - Preserves data lineage and enables reprocessing
--   - Serves as the source of truth for all downstream layers
--   - Immutable - data is append-only or fully replaced
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.RAW
  COMMENT = 'RAW layer: Unprocessed data loaded directly from S3, preserving original format and values';

-- STAGING LAYER (Silver)
-- Purpose: Apply business rules, data quality checks, and standardization
-- Characteristics:
--   - Data is cleaned (nulls handled, duplicates removed)
--   - Data types are standardized and validated
--   - Business logic is applied (calculations, derivations)
--   - Serves as the foundation for multiple downstream use cases
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.STAGING
  COMMENT = 'STAGING layer: Cleaned, validated, and transformed data ready for business consumption';

-- PRODUCTION LAYER (Gold)
-- Purpose: Host optimized dimensional model for analytical queries
-- Characteristics:
--   - Denormalized for query performance
--   - Implements dimensional modeling (facts and dimensions)
--   - Aggregated and pre-computed where beneficial
--   - Optimized for specific business questions and dashboards
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.PRODUCTION
  COMMENT = 'PRODUCTION layer: Business-ready dimensional model (facts/dimensions) optimized for analytics';

-- Confirm schema creation
SELECT 'All schemas created successfully' AS status;

/*******************************************************************************
 * SET DEFAULT CONTEXT
 ******************************************************************************/

-- Set the database context for subsequent operations
-- This ensures all following commands operate within ECOMMERCE_DW
USE DATABASE ECOMMERCE_DW;

-- Set default schema to RAW for development work
-- This is where data ingestion typically begins
USE SCHEMA RAW;

-- Display current context for verification
SELECT CURRENT_DATABASE() AS current_database,
       CURRENT_SCHEMA() AS current_schema,
       CURRENT_ROLE() AS current_role;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- List all schemas in the database to verify creation
SHOW SCHEMAS IN DATABASE ECOMMERCE_DW;

-- Display database properties
SHOW DATABASES LIKE 'ECOMMERCE_DW';

/*******************************************************************************
 * ARCHITECTURE NOTES
 *
 * Why Three Layers?
 * -----------------
 * 1. RAW: Enables data recovery and reprocessing without reloading from source
 * 2. STAGING: Separates data quality concerns from analytics optimization
 * 3. PRODUCTION: Provides performance-optimized structure for end users
 *
 * Data Flow:
 * ---------
 * S3 CSV Files → COPY INTO → RAW tables → Transformations → STAGING tables
 *                                                         → JOIN/AGGREGATE → PRODUCTION tables
 *
 * Benefits:
 * --------
 * - Clear separation of concerns (ingestion vs. transformation vs. serving)
 * - Enables parallel development (different teams can work on different layers)
 * - Simplifies debugging (can isolate issues to specific transformation steps)
 * - Facilitates testing (can validate each layer independently)
 * - Supports incremental loading patterns (can refresh layers independently)
 *
 * Next Steps:
 * ----------
 * 1. Create file formats (03_create_file_formats.sql)
 * 2. Create external stages pointing to S3
 * 3. Define RAW layer tables
 * 4. Implement STAGING transformations
 * 5. Build PRODUCTION dimensional model
 ******************************************************************************/
