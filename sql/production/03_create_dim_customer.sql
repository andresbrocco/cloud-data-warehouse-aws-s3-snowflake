/*******************************************************************************
 * Script: 03_create_dim_customer.sql
 * Purpose: Create and populate the customer dimension table with SCD Type 2
 *
 * Description:
 *   This script creates a customer dimension table with Slowly Changing Dimension
 *   Type 2 (SCD2) support. The customer dimension enables customer segmentation,
 *   lifetime value analysis, and cohort studies.
 *
 *   SCD Type 2 allows tracking customer attribute changes over time by creating
 *   new rows for each change, maintaining historical versions of each customer.
 *
 * Key Features:
 *   - AUTOINCREMENT primary key (customer_key) for surrogate key generation
 *   - customer_id: Business key from source system
 *   - country_key: Foreign key to dim_country (snowflake schema)
 *   - first_order_date, last_order_date: Customer lifecycle metrics
 *   - total_lifetime_orders: Aggregated customer behavior metric
 *   - SCD Type 2 columns: _effective_from, _effective_to, _is_current
 *
 * Data Flow:
 *   STAGING.stg_orders → Aggregate by customer_id → PRODUCTION.dim_customer
 *   PRODUCTION.dim_country → Join on country_name → country_key FK
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. STAGING layer populated (sql/staging/02_load_staging_from_raw.sql)
 *   3. dim_country created (sql/production/02_create_dim_country.sql)
 *   4. Valid data in STAGING.stg_orders (WHERE is_valid = TRUE)
 *
 * Execution Instructions:
 *   1. Execute this entire script in Snowflake worksheet
 *   2. Verify table creation: SELECT COUNT(*) FROM ECOMMERCE_DW.PRODUCTION.dim_customer;
 *   3. Expected result: ~4,000 distinct customers from the dataset
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
 * CUSTOMER DIMENSION TABLE
 *
 * Design Rationale:
 * -----------------
 * The customer dimension is a core dimension for understanding customer behavior,
 * segmentation, and lifetime value. It includes:
 * 1. Business identifiers (customer_id from source)
 * 2. Foreign key relationships (country_key to dim_country)
 * 3. Pre-aggregated customer metrics (order counts, dates)
 * 4. SCD Type 2 infrastructure for tracking changes over time
 *
 * Why Use SCD Type 2?
 * -------------------
 * SCD Type 2 allows us to track how customer attributes change over time:
 * - If a customer moves countries, we create a new row
 * - The old row is expired (_effective_to is set, _is_current = FALSE)
 * - The new row becomes current (_is_current = TRUE)
 * - Historical analysis can query specific time periods using effective dates
 *
 * For this initial load, all customers are inserted with:
 * - _effective_from = first_order_date (when customer first appeared)
 * - _effective_to = NULL (no end date, currently active)
 * - _is_current = TRUE (active version)
 *
 * SCD Type 2 Columns Explained:
 * -----------------------------
 * _effective_from (TIMESTAMP_NTZ):
 *   - When this version of the customer record became active
 *   - For initial load, set to first_order_date
 *   - For future changes, set to the date of the change
 *
 * _effective_to (TIMESTAMP_NTZ):
 *   - When this version of the customer record expired
 *   - NULL means the record is currently active
 *   - Set when a new version is created (customer moves, attributes change)
 *
 * _is_current (BOOLEAN DEFAULT TRUE):
 *   - Flag indicating if this is the active version of the customer
 *   - TRUE = current/active version (most recent)
 *   - FALSE = historical version (expired)
 *   - Simplifies queries: WHERE _is_current = TRUE gets latest version
 *
 * Why Underscore Prefix on SCD Columns?
 * -------------------------------------
 * The underscore prefix (_effective_from, _effective_to, _is_current) indicates
 * these are metadata columns for internal tracking, not business attributes.
 * This is a common naming convention in dimensional modeling.
 *
 * Snowflake Schema Pattern:
 * ------------------------
 * This dimension demonstrates snowflake schema normalization:
 *
 *   fact_sales → dim_customer → dim_country
 *
 * The customer dimension references dim_country via country_key foreign key,
 * normalizing country attributes to reduce redundancy.
 *
 * Pre-Aggregated Metrics:
 * ----------------------
 * first_order_date: Customer's first transaction date (acquisition date)
 * last_order_date: Customer's most recent transaction date (recency)
 * total_lifetime_orders: Count of distinct orders placed (frequency)
 *
 * These metrics support RFM analysis (Recency, Frequency, Monetary) and
 * customer lifetime value calculations.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.dim_customer (
  -- Primary Key (Surrogate)
  customer_key              INTEGER AUTOINCREMENT PRIMARY KEY,

  -- Business Key
  customer_id               INTEGER NOT NULL,

  -- Foreign Key to dim_country
  country_key               INTEGER,  -- References dim_country.country_key

  -- Customer Lifecycle Metrics (Pre-Aggregated)
  first_order_date          DATE,
  last_order_date           DATE,
  total_lifetime_orders     INTEGER,

  -- SCD Type 2 Columns (Tracking Historical Changes)
  _effective_from           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _effective_to             TIMESTAMP_NTZ,  -- NULL = currently active
  _is_current               BOOLEAN DEFAULT TRUE
)
COMMENT = 'Customer dimension with SCD Type 2: tracks customer attributes and lifecycle metrics';

/*******************************************************************************
 * POPULATE CUSTOMER DIMENSION
 *
 * Strategy:
 * --------
 * 1. Aggregate customer metrics from STAGING.stg_orders:
 *    - First order date: MIN(invoice_date)
 *    - Last order date: MAX(invoice_date)
 *    - Total orders: COUNT(DISTINCT invoice_no)
 * 2. Join with dim_country to get country_key foreign key
 * 3. Only include customers with customer_id IS NOT NULL
 * 4. Only include valid records (is_valid = TRUE)
 * 5. Initialize SCD Type 2 columns:
 *    - _effective_from = first_order_date (customer lifecycle start)
 *    - _effective_to = NULL (currently active)
 *    - _is_current = TRUE (active version)
 *
 * Data Source:
 * -----------
 * STAGING.stg_orders contains transactional data with customer_id and country.
 * We aggregate to customer level to create one row per customer.
 *
 * Guest Transactions:
 * ------------------
 * Records where customer_id IS NULL represent guest checkouts and are excluded
 * from the customer dimension. These transactions will still appear in fact_sales
 * but with customer_key = NULL or a special "Unknown Customer" surrogate key.
 *
 * Initial Load vs. Incremental Updates:
 * -------------------------------------
 * This script performs an INITIAL LOAD, creating the first version of each customer.
 * Future incremental loads would:
 * 1. Detect customer attribute changes (e.g., country changed)
 * 2. Expire the old row (set _effective_to, _is_current = FALSE)
 * 3. Insert new row with updated attributes (_is_current = TRUE)
 *
 * For this portfolio project, we implement the initial load only. Incremental
 * SCD Type 2 updates would require a separate ETL script with change detection logic.
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.dim_customer (
  customer_id,
  country_key,
  first_order_date,
  last_order_date,
  total_lifetime_orders,
  _effective_from,
  _effective_to,
  _is_current
)
WITH customer_aggregates AS (
  -- Aggregate customer metrics from staging orders
  SELECT
    customer_id,
    country,
    MIN(invoice_date::DATE) AS first_order_date,
    MAX(invoice_date::DATE) AS last_order_date,
    COUNT(DISTINCT invoice_no) AS total_lifetime_orders
  FROM ECOMMERCE_DW.STAGING.stg_orders
  WHERE customer_id IS NOT NULL  -- Exclude guest transactions
    AND is_valid = TRUE          -- Only include validated records
  GROUP BY customer_id, country
)
SELECT
  ca.customer_id,
  c.country_key,               -- Foreign key lookup
  ca.first_order_date,
  ca.last_order_date,
  ca.total_lifetime_orders,
  ca.first_order_date AS _effective_from,  -- Customer became active on first order
  NULL AS _effective_to,                   -- Currently active (no end date)
  TRUE AS _is_current                      -- Active version
FROM customer_aggregates ca
LEFT JOIN ECOMMERCE_DW.PRODUCTION.dim_country c
  ON ca.country = c.country_name
ORDER BY ca.customer_id;

-- Confirm population
SELECT 'dim_customer populated successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.dim_customer;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.dim_customer;

-- View sample customers with all attributes
SELECT
  customer_key,
  customer_id,
  country_key,
  first_order_date,
  last_order_date,
  total_lifetime_orders,
  _effective_from,
  _effective_to,
  _is_current
FROM ECOMMERCE_DW.PRODUCTION.dim_customer
LIMIT 10;

-- Verify all customers are currently active (initial load)
SELECT
  _is_current,
  COUNT(*) AS customer_count
FROM ECOMMERCE_DW.PRODUCTION.dim_customer
GROUP BY _is_current;

-- Customer segmentation by order count
SELECT
  CASE
    WHEN total_lifetime_orders = 1 THEN 'One-Time Buyer'
    WHEN total_lifetime_orders BETWEEN 2 AND 5 THEN 'Occasional Buyer'
    WHEN total_lifetime_orders BETWEEN 6 AND 10 THEN 'Regular Customer'
    ELSE 'VIP Customer'
  END AS customer_segment,
  COUNT(*) AS customer_count
FROM ECOMMERCE_DW.PRODUCTION.dim_customer
GROUP BY customer_segment
ORDER BY MIN(total_lifetime_orders);

-- Verify foreign key relationship with dim_country
SELECT
  c.country_name,
  c.region,
  COUNT(cu.customer_key) AS customer_count
FROM ECOMMERCE_DW.PRODUCTION.dim_customer cu
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_country c
  ON cu.country_key = c.country_key
WHERE cu._is_current = TRUE
GROUP BY c.country_name, c.region
ORDER BY customer_count DESC
LIMIT 10;

-- Check for NULL country_keys (customers without country match)
SELECT
  COUNT(*) AS customers_without_country
FROM ECOMMERCE_DW.PRODUCTION.dim_customer
WHERE country_key IS NULL;

-- Top 10 customers by total lifetime orders
SELECT
  customer_id,
  total_lifetime_orders,
  first_order_date,
  last_order_date,
  DATEDIFF(DAY, first_order_date, last_order_date) AS customer_tenure_days
FROM ECOMMERCE_DW.PRODUCTION.dim_customer
WHERE _is_current = TRUE
ORDER BY total_lifetime_orders DESC
LIMIT 10;

/*******************************************************************************
 * USAGE NOTES
 *
 * Joining with Fact Tables:
 * -------------------------
 * The fact_sales table will reference this dimension via customer_key:
 *
 * SELECT
 *   c.customer_id,
 *   c.total_lifetime_orders,
 *   SUM(f.total_amount) AS total_revenue
 * FROM fact_sales f
 * INNER JOIN dim_customer c ON f.customer_key = c.customer_key
 * WHERE c._is_current = TRUE  -- Only use current customer versions
 * GROUP BY c.customer_id, c.total_lifetime_orders;
 *
 * SCD Type 2 Query Patterns:
 * -------------------------
 * 1. Current customers only (most common):
 *    WHERE _is_current = TRUE
 *
 * 2. Historical point-in-time query (as of specific date):
 *    WHERE '2010-06-15' BETWEEN _effective_from AND COALESCE(_effective_to, '9999-12-31')
 *
 * 3. All versions of a specific customer (audit trail):
 *    WHERE customer_id = 12345
 *    ORDER BY _effective_from
 *
 * Common Analysis Patterns:
 * ------------------------
 * 1. Customer segmentation by order frequency (total_lifetime_orders)
 * 2. Cohort analysis by first_order_date (acquisition cohorts)
 * 3. Churn analysis using last_order_date (recency)
 * 4. Regional customer distribution via country_key → dim_country
 * 5. Customer lifetime value calculations (combine with fact_sales revenue)
 *
 * RFM Analysis Support:
 * --------------------
 * This dimension supports RFM (Recency, Frequency, Monetary) analysis:
 * - Recency: last_order_date (how recently did customer purchase?)
 * - Frequency: total_lifetime_orders (how often do they purchase?)
 * - Monetary: Join with fact_sales to calculate total revenue per customer
 *
 * SCD Type 2 Incremental Update Pattern (Future Implementation):
 * --------------------------------------------------------------
 * When a customer's country changes:
 *
 * 1. Identify changed customers:
 *    SELECT customer_id, new_country
 *    FROM staging_new_data
 *    WHERE (customer_id, country) NOT IN (
 *      SELECT customer_id, country FROM dim_customer WHERE _is_current = TRUE
 *    );
 *
 * 2. Expire old version:
 *    UPDATE dim_customer
 *    SET _effective_to = CURRENT_TIMESTAMP(), _is_current = FALSE
 *    WHERE customer_id IN (changed_customers) AND _is_current = TRUE;
 *
 * 3. Insert new version:
 *    INSERT INTO dim_customer (customer_id, country_key, _effective_from, _is_current)
 *    VALUES (12345, new_country_key, CURRENT_TIMESTAMP(), TRUE);
 *
 * Performance Considerations:
 * --------------------------
 * - AUTOINCREMENT primary key enables efficient joins
 * - _is_current flag simplifies queries (no date range filters needed)
 * - Pre-aggregated metrics avoid repeated calculations
 * - Foreign key to dim_country reduces data redundancy
 *
 * Future Enhancements:
 * -------------------
 * For a production implementation, consider adding:
 * - Email and contact information (with PII protection)
 * - Customer segments (VIP, regular, one-time, at-risk)
 * - Marketing preferences and channel attribution
 * - Customer acquisition source (marketing campaign, referral, organic)
 * - Credit limit or payment terms for B2B scenarios
 * - Customer status flags (active, inactive, churned)
 ******************************************************************************/
