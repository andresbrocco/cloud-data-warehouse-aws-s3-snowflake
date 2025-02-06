/*******************************************************************************
 * Script: 02_create_dim_country.sql
 * Purpose: Create and populate the country dimension table
 *
 * Description:
 *   This script creates a country dimension table extracted from the STAGING
 *   layer. The country dimension enables geographic analysis, regional reporting,
 *   and international market segmentation.
 *
 *   This dimension is denormalized from the customer dimension (snowflake schema
 *   pattern) to reduce redundancy and enable shared lookups across multiple
 *   fact tables.
 *
 * Key Features:
 *   - AUTOINCREMENT primary key (country_key) for surrogate key generation
 *   - country_name: Full country name as it appears in source data
 *   - country_code: 2-letter ISO code for standardization (simplified approach)
 *   - region: Geographic grouping for high-level analysis
 *   - Type 1 SCD: No history tracking (country attributes don't change)
 *
 * Data Flow:
 *   STAGING.stg_orders.country → PRODUCTION.dim_country → PRODUCTION.dim_customer.country_key
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. STAGING layer populated (sql/staging/02_load_staging_from_raw.sql)
 *   3. Valid data in STAGING.stg_orders (WHERE is_valid = TRUE)
 *
 * Execution Instructions:
 *   1. Execute this entire script in Snowflake worksheet
 *   2. Verify table creation: SELECT * FROM ECOMMERCE_DW.PRODUCTION.dim_country;
 *   3. Expected result: ~40 distinct countries from the dataset
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-06
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;
USE ROLE SYSADMIN;

/*******************************************************************************
 * COUNTRY DIMENSION TABLE
 *
 * Design Rationale:
 * -----------------
 * The country dimension is normalized out of the customer dimension following
 * the snowflake schema pattern. This reduces data redundancy (storing "United Kingdom"
 * once instead of thousands of times for each customer) and enables:
 * 1. Consistent country name standardization
 * 2. Regional groupings for high-level analysis
 * 3. Country-level attributes (codes, regions) without customer duplication
 *
 * Why AUTOINCREMENT for country_key?
 * ----------------------------------
 * - Surrogate key: Independent of business data changes
 * - Simplicity: Snowflake automatically generates sequential integers
 * - Performance: Integer keys are efficient for joins
 * - Flexibility: Can add countries without worrying about key collisions
 *
 * Why Include country_code and region?
 * ------------------------------------
 * - country_code: Enables integration with external systems (ISO standards)
 * - region: Supports high-level geographic rollups (Europe, Asia, Americas)
 * - These attributes could be maintained in a reference table but included
 *   here for simplicity in a portfolio project
 *
 * Snowflake Schema Pattern:
 * ------------------------
 * This dimension demonstrates snowflake schema normalization:
 *
 *   fact_sales → dim_customer → dim_country
 *
 * Instead of denormalizing country into dim_customer (star schema), we
 * normalize it into a separate dimension to reduce redundancy and enable
 * shared country lookups across multiple dimensions if needed.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.dim_country (
  -- Primary Key (Surrogate)
  country_key         INTEGER AUTOINCREMENT PRIMARY KEY,

  -- Country Attributes
  country_name        VARCHAR(100) NOT NULL UNIQUE,   -- Full country name
  country_code        VARCHAR(2),                     -- ISO 3166-1 alpha-2 code (simplified)
  region              VARCHAR(50)                     -- Geographic region grouping
)
COMMENT = 'Country dimension: geographic attributes for regional analysis';

/*******************************************************************************
 * POPULATE COUNTRY DIMENSION
 *
 * Strategy:
 * --------
 * Extract distinct countries from STAGING.stg_orders where:
 * 1. country IS NOT NULL (filter out missing values)
 * 2. is_valid = TRUE (only include quality-validated records)
 * 3. ORDER BY country_name (alphabetical for readability)
 *
 * Data Source:
 * -----------
 * The e-commerce dataset contains UK-based transactions with international
 * customers. Expected countries include:
 * - United Kingdom (majority)
 * - European countries (Germany, France, Netherlands, etc.)
 * - Other international markets (Australia, Japan, USA, etc.)
 *
 * Simplified Approach for Portfolio Project:
 * -----------------------------------------
 * In production, you would:
 * 1. Use a reference table with full ISO country codes
 * 2. Implement standardization logic for name variations
 * 3. Add latitude/longitude for geographic mapping
 * 4. Include currency codes and language information
 *
 * For this portfolio demonstration, we use a simple extraction with
 * placeholder values for country_code and region. A real implementation
 * would use CASE statements or lookup tables to populate these correctly.
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.dim_country (country_name, country_code, region)
SELECT DISTINCT
  country AS country_name,
  -- Placeholder for country_code (would use lookup table in production)
  NULL AS country_code,
  -- Placeholder for region (would use lookup table or CASE statement in production)
  CASE
    WHEN country IN ('United Kingdom', 'Germany', 'France', 'Netherlands',
                     'Belgium', 'Spain', 'Italy', 'Switzerland', 'Portugal',
                     'Austria', 'Denmark', 'Norway', 'Sweden', 'Finland',
                     'Poland', 'Greece', 'Ireland', 'Czech Republic') THEN 'Europe'
    WHEN country IN ('Australia', 'New Zealand') THEN 'Oceania'
    WHEN country IN ('Japan', 'Singapore', 'Hong Kong', 'Korea', 'Israel',
                     'United Arab Emirates', 'Saudi Arabia', 'Lebanon') THEN 'Asia'
    WHEN country IN ('USA', 'United States', 'Canada', 'Brazil') THEN 'Americas'
    ELSE 'Other'
  END AS region
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE country IS NOT NULL
  AND is_valid = TRUE
ORDER BY country_name;

-- Confirm population
SELECT 'dim_country populated successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.dim_country;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.dim_country;

-- View all countries with their assigned regions
SELECT
  country_key,
  country_name,
  country_code,
  region
FROM ECOMMERCE_DW.PRODUCTION.dim_country
ORDER BY region, country_name;

-- Count countries by region
SELECT
  region,
  COUNT(*) AS country_count
FROM ECOMMERCE_DW.PRODUCTION.dim_country
GROUP BY region
ORDER BY country_count DESC;

-- Verify United Kingdom is present (should be the primary market)
SELECT *
FROM ECOMMERCE_DW.PRODUCTION.dim_country
WHERE country_name = 'United Kingdom';

-- Check for any NULL country_names (should be none)
SELECT COUNT(*) AS null_countries
FROM ECOMMERCE_DW.PRODUCTION.dim_country
WHERE country_name IS NULL;

/*******************************************************************************
 * USAGE NOTES
 *
 * Joining with Customer Dimension:
 * --------------------------------
 * The customer dimension will reference this table via country_key:
 *
 * CREATE TABLE dim_customer (
 *   customer_key INTEGER PRIMARY KEY,
 *   customer_id INTEGER,
 *   country_key INTEGER,  -- Foreign key to dim_country
 *   ...
 * );
 *
 * Example join for regional customer analysis:
 * SELECT
 *   c.region,
 *   COUNT(DISTINCT cu.customer_key) AS customer_count
 * FROM dim_customer cu
 * INNER JOIN dim_country c ON cu.country_key = c.country_key
 * GROUP BY c.region;
 *
 * Common Analysis Patterns:
 * ------------------------
 * 1. Regional sales comparison (GROUP BY region)
 * 2. Country-specific metrics (WHERE country_name = 'Germany')
 * 3. International vs domestic analysis (CASE WHEN region = 'Europe')
 * 4. Market expansion tracking (COUNT DISTINCT country_key over time)
 *
 * Snowflake Schema Benefits:
 * -------------------------
 * - Reduced storage: Country name stored once, not replicated per customer
 * - Consistent attributes: Regional groupings applied uniformly
 * - Easier updates: Changing a country's region updates all customers automatically
 * - Shared dimension: Can be used by multiple fact tables if needed
 *
 * Type 1 SCD Approach:
 * -------------------
 * This dimension uses Type 1 SCD (overwrite on change) because:
 * - Country names rarely change
 * - Regional assignments are relatively stable
 * - Historical country changes are not analytically significant for this use case
 *
 * If you needed to track country attribute changes over time (e.g., EU membership
 * status changes), you would implement Type 2 SCD with effective_from/effective_to.
 *
 * Performance Considerations:
 * --------------------------
 * - Small dimension (typically 40-50 rows) means it's always cached
 * - UNIQUE constraint on country_name prevents duplicates
 * - INTEGER primary key enables efficient joins with dim_customer
 * - AUTOINCREMENT simplifies key management
 *
 * Future Enhancements:
 * -------------------
 * For a production implementation, consider adding:
 * - ISO 3166-1 alpha-2 codes (GB, FR, DE, etc.)
 * - Currency codes (GBP, EUR, USD)
 * - Latitude/longitude for geographic visualization
 * - Timezone information
 * - Language codes
 * - Population and GDP for market sizing analysis
 ******************************************************************************/
