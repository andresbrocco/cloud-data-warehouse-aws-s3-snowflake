/*******************************************************************************
 * Script: 01_create_dim_date.sql
 * Purpose: Create and populate the date dimension table
 *
 * Description:
 *   This script creates a date dimension table spanning from 2009-01-01 to
 *   2012-12-31, covering the full range of e-commerce transactions in our dataset.
 *   The date dimension enables time-based analysis, trending, seasonality studies,
 *   and calendar-aware reporting.
 *
 *   The date dimension is a Type 1 Slowly Changing Dimension (no history tracking)
 *   because calendar attributes don't change over time.
 *
 * Key Features:
 *   - date_key as INTEGER in YYYYMMDD format (20091201) for efficient joins
 *   - Full calendar hierarchy: year, quarter, month, week, day
 *   - Day of week names and numbers for temporal pattern analysis
 *   - Weekend flag for business vs. leisure shopping analysis
 *   - Uses Snowflake's GENERATOR function to create date series
 *
 * Data Flow:
 *   Generated dimension → PRODUCTION.dim_date → Joined by fact_sales.invoice_date_key
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. PRODUCTION schema exists
 *
 * Execution Instructions:
 *   1. Execute this entire script in Snowflake worksheet
 *   2. Verify table creation: SELECT COUNT(*) FROM ECOMMERCE_DW.PRODUCTION.dim_date;
 *   3. Expected result: 1,461 rows (4 years including leap year 2012)
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
 * DATE DIMENSION TABLE
 *
 * Design Rationale:
 * -----------------
 * The date dimension is one of the most critical dimensions in any data warehouse.
 * It enables time-based slicing, trending, and period-over-period comparisons.
 *
 * Why Use an INTEGER date_key Instead of DATE?
 * --------------------------------------------
 * 1. Performance: Integer joins are faster than date comparisons
 * 2. Compatibility: Works across all BI tools and query engines
 * 3. Simplicity: YYYYMMDD format is human-readable (20091201 = Dec 1, 2009)
 * 4. Standard: Industry best practice for dimensional modeling
 *
 * Date Range Selection:
 * --------------------
 * 2009-01-01 to 2012-12-31 chosen to cover our e-commerce dataset timespan
 * plus a buffer for potential future data. This generates 1,461 dates.
 *
 * Calendar Attributes Included:
 * ----------------------------
 * - Date identifiers: date_key (PK), date (actual date)
 * - Year hierarchy: year, quarter, month
 * - Month details: month_name (January, February, etc.)
 * - Day details: day, day_of_week, day_name
 * - Business logic: is_weekend (TRUE for Saturday/Sunday)
 *
 * Snowflake DAYOFWEEK Convention:
 * ------------------------------
 * 0 = Sunday, 1 = Monday, ..., 6 = Saturday
 * We use this to identify weekends: day_of_week IN (0, 6)
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.dim_date (
  -- Primary Key
  date_key            INTEGER PRIMARY KEY,           -- YYYYMMDD format (e.g., 20091201)

  -- Actual Date
  date                DATE NOT NULL,                 -- Full date value for calculations

  -- Year Hierarchy
  year                INTEGER NOT NULL,              -- 4-digit year (2009, 2010, etc.)
  quarter             INTEGER NOT NULL,              -- Quarter number (1, 2, 3, 4)
  month               INTEGER NOT NULL,              -- Month number (1-12)

  -- Month Details
  month_name          VARCHAR(20) NOT NULL,          -- Full month name (January, February, etc.)

  -- Day Details
  day                 INTEGER NOT NULL,              -- Day of month (1-31)
  day_of_week         INTEGER NOT NULL,              -- Day of week (0=Sunday, 6=Saturday)
  day_name            VARCHAR(20) NOT NULL,          -- Full day name (Monday, Tuesday, etc.)

  -- Business Logic Flags
  is_weekend          BOOLEAN NOT NULL               -- TRUE if Saturday or Sunday
)
COMMENT = 'Date dimension: calendar attributes for time-based analysis (2009-2012)';

/*******************************************************************************
 * POPULATE DATE DIMENSION
 *
 * Strategy:
 * --------
 * Use Snowflake's GENERATOR function to create a sequence of dates from
 * 2009-01-01 to 2012-12-31 (1,461 days including leap year 2012).
 *
 * GENERATOR Function:
 * ------------------
 * GENERATOR(ROWCOUNT => N) creates N rows that we can use with ROW_NUMBER()
 * to generate sequential dates using DATEADD.
 *
 * Formula:
 * -------
 * Start date: 2009-01-01
 * For each row N: DATEADD(DAY, N, '2009-01-01') generates the Nth day
 * End date: 2012-12-31 (when N = 1,460, since we start at 0)
 *
 * Date Attribute Functions:
 * ------------------------
 * - YEAR(date): Extract year
 * - QUARTER(date): Extract quarter (1-4)
 * - MONTH(date): Extract month (1-12)
 * - MONTHNAME(date): Get month name (January, etc.)
 * - DAY(date): Extract day of month (1-31)
 * - DAYOFWEEK(date): Get day of week (0=Sunday, 6=Saturday)
 * - DAYNAME(date): Get day name (Monday, etc.)
 * - TO_NUMBER(TO_CHAR(date, 'YYYYMMDD')): Convert date to integer key
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.dim_date
WITH date_series AS (
  -- Generate sequence of dates from 2009-01-01 to 2012-12-31
  SELECT
    DATEADD(DAY,
            ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,  -- Start from 0
            '2009-01-01'::DATE) AS generated_date
  FROM TABLE(GENERATOR(ROWCOUNT => 1461))  -- 1,461 days = ~4 years
)
SELECT
  -- Primary Key: YYYYMMDD format
  TO_NUMBER(TO_CHAR(generated_date, 'YYYYMMDD')) AS date_key,

  -- Actual Date
  generated_date AS date,

  -- Year Hierarchy
  YEAR(generated_date) AS year,
  QUARTER(generated_date) AS quarter,
  MONTH(generated_date) AS month,

  -- Month Details
  MONTHNAME(generated_date) AS month_name,

  -- Day Details
  DAY(generated_date) AS day,
  DAYOFWEEK(generated_date) AS day_of_week,      -- 0=Sunday, 6=Saturday
  DAYNAME(generated_date) AS day_name,

  -- Business Logic Flags
  CASE
    WHEN DAYOFWEEK(generated_date) IN (0, 6) THEN TRUE  -- Sunday or Saturday
    ELSE FALSE
  END AS is_weekend

FROM date_series
ORDER BY generated_date;

-- Confirm population
SELECT 'dim_date populated successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.dim_date;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.dim_date;

-- Verify row count (should be 1,461)
SELECT COUNT(*) AS total_rows
FROM ECOMMERCE_DW.PRODUCTION.dim_date;

-- Verify date range
SELECT
  MIN(date) AS earliest_date,
  MAX(date) AS latest_date,
  DATEDIFF(DAY, MIN(date), MAX(date)) + 1 AS total_days
FROM ECOMMERCE_DW.PRODUCTION.dim_date;

-- Sample records to verify attributes
SELECT
  date_key,
  date,
  year,
  quarter,
  month_name,
  day_name,
  is_weekend
FROM ECOMMERCE_DW.PRODUCTION.dim_date
WHERE date IN ('2009-01-01', '2010-06-15', '2011-12-25', '2012-12-31')
ORDER BY date;

-- Count weekdays vs weekends
SELECT
  is_weekend,
  COUNT(*) AS day_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ECOMMERCE_DW.PRODUCTION.dim_date
GROUP BY is_weekend
ORDER BY is_weekend;

-- Verify all years are represented
SELECT
  year,
  COUNT(*) AS days_in_year,
  MIN(date) AS year_start,
  MAX(date) AS year_end
FROM ECOMMERCE_DW.PRODUCTION.dim_date
GROUP BY year
ORDER BY year;

/*******************************************************************************
 * USAGE NOTES
 *
 * Joining with Fact Tables:
 * -------------------------
 * In fact tables, store invoice_date_key as INTEGER in YYYYMMDD format.
 *
 * Example join:
 * SELECT
 *   d.year,
 *   d.month_name,
 *   SUM(f.total_amount) AS monthly_revenue
 * FROM fact_sales f
 * INNER JOIN dim_date d ON f.invoice_date_key = d.date_key
 * GROUP BY d.year, d.month_name
 * ORDER BY d.year, d.month;
 *
 * Common Analysis Patterns:
 * ------------------------
 * 1. Year-over-year comparisons (d.year)
 * 2. Quarterly trends (d.quarter)
 * 3. Seasonal patterns (d.month_name)
 * 4. Day-of-week patterns (d.day_name)
 * 5. Weekend vs weekday behavior (d.is_weekend)
 *
 * Performance Considerations:
 * --------------------------
 * - date_key as INTEGER enables fast joins (more efficient than DATE joins)
 * - Small table (1,461 rows) means it's always cached in memory
 * - No need for explicit indexing in Snowflake (micro-partitions handle this)
 *
 * Maintenance:
 * -----------
 * - This is a static dimension (no updates needed unless date range changes)
 * - If data extends beyond 2012, re-run this script with updated ROWCOUNT
 * - No SCD (Slowly Changing Dimension) tracking needed - dates don't change
 *
 * Extension Ideas:
 * ---------------
 * Future enhancements could include:
 * - Fiscal calendar attributes (fiscal_year, fiscal_quarter)
 * - Holiday indicators (is_holiday flag)
 * - Business day calculations (is_business_day)
 * - Week-ending date for weekly aggregations
 * - ISO week numbering for international reporting
 ******************************************************************************/
