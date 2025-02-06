/*******************************************************************************
 * Script: 05_create_dim_product.sql
 * Purpose: Create and populate the product dimension table with SCD Type 2
 *
 * Description:
 *   This script creates a product dimension table with Slowly Changing Dimension
 *   Type 2 (SCD2) support. The product dimension enables product analysis,
 *   pricing trends, and merchandising insights.
 *
 *   SCD Type 2 allows tracking product attribute changes over time (e.g., price
 *   changes, description updates) by creating new rows for each change.
 *
 * Key Features:
 *   - AUTOINCREMENT primary key (product_key) for surrogate key generation
 *   - stock_code: Business key from source system (product SKU)
 *   - description: Product name/description
 *   - category_key: Foreign key to dim_category (snowflake schema)
 *   - unit_price: Average unit price from historical transactions
 *   - first_sold_date: When product first appeared in transactions
 *   - SCD Type 2 columns: _effective_from, _effective_to, _is_current
 *
 * Data Flow:
 *   STAGING.stg_orders → Aggregate by stock_code → PRODUCTION.dim_product
 *   PRODUCTION.dim_category → Default assignment → category_key FK
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. STAGING layer populated (sql/staging/02_load_staging_from_raw.sql)
 *   3. dim_category created (sql/production/04_create_dim_category.sql)
 *   4. Valid data in STAGING.stg_orders (WHERE is_valid = TRUE)
 *
 * Execution Instructions:
 *   1. Execute this entire script in Snowflake worksheet
 *   2. Verify table creation: SELECT COUNT(*) FROM ECOMMERCE_DW.PRODUCTION.dim_product;
 *   3. Expected result: ~3,000-4,000 distinct products from the dataset
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
 * PRODUCT DIMENSION TABLE
 *
 * Design Rationale:
 * -----------------
 * The product dimension is a core dimension for understanding product performance,
 * pricing trends, and merchandising effectiveness. It includes:
 * 1. Business identifiers (stock_code from source)
 * 2. Product attributes (description, category)
 * 3. Pricing information (unit_price as average from transactions)
 * 4. Product lifecycle tracking (first_sold_date)
 * 5. SCD Type 2 infrastructure for tracking changes over time
 *
 * Why Use SCD Type 2?
 * -------------------
 * SCD Type 2 allows us to track how product attributes change over time:
 * - If a product's price changes significantly, we can create a new version
 * - If a product's description is updated, we maintain the old version
 * - Historical analysis can query products as they existed at specific points in time
 *
 * For this initial load, all products are inserted with:
 * - _effective_from = first_sold_date (when product first appeared)
 * - _effective_to = NULL (no end date, currently active)
 * - _is_current = TRUE (active version)
 *
 * SCD Type 2 Columns Explained:
 * -----------------------------
 * _effective_from (TIMESTAMP_NTZ):
 *   - When this version of the product record became active
 *   - For initial load, set to first_sold_date
 *   - For future changes, set to the date of the change
 *
 * _effective_to (TIMESTAMP_NTZ):
 *   - When this version of the product record expired
 *   - NULL means the record is currently active
 *   - Set when a new version is created (price change, description update)
 *
 * _is_current (BOOLEAN DEFAULT TRUE):
 *   - Flag indicating if this is the active version of the product
 *   - TRUE = current/active version (most recent)
 *   - FALSE = historical version (expired)
 *   - Simplifies queries: WHERE _is_current = TRUE gets latest version
 *
 * Snowflake Schema Pattern:
 * ------------------------
 * This dimension demonstrates snowflake schema normalization:
 *
 *   fact_sales → dim_product → dim_category
 *
 * The product dimension references dim_category via category_key foreign key,
 * normalizing category attributes to reduce redundancy.
 *
 * Price Handling Strategy:
 * -----------------------
 * unit_price is calculated as AVG(unit_price) from all transactions for this product.
 * This represents the typical price point for the product during the data period.
 *
 * Alternative approaches:
 * - Store mode (most common price)
 * - Store latest price (most recent transaction)
 * - Store price ranges (min/max)
 *
 * For this portfolio project, average price provides a reasonable baseline.
 * Production implementations might track price as a separate fact table or
 * implement more sophisticated price change tracking.
 *
 * Category Assignment Strategy:
 * ----------------------------
 * All products are initially assigned to category_key = 1 (General Merchandise)
 * as a default. In production, you would implement:
 * - Machine learning classification based on product descriptions
 * - Rule-based keyword matching
 * - Manual product master data management
 * - Integration with external product taxonomies
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.dim_product (
  -- Primary Key (Surrogate)
  product_key           INTEGER AUTOINCREMENT PRIMARY KEY,

  -- Business Key
  stock_code            VARCHAR(50) NOT NULL,

  -- Product Attributes
  description           VARCHAR(500),
  category_key          INTEGER,  -- References dim_category.category_key
  unit_price            DECIMAL(10,2),

  -- Product Lifecycle
  first_sold_date       DATE,

  -- SCD Type 2 Columns (Tracking Historical Changes)
  _effective_from       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _effective_to         TIMESTAMP_NTZ,  -- NULL = currently active
  _is_current           BOOLEAN DEFAULT TRUE
)
COMMENT = 'Product dimension with SCD Type 2: tracks product attributes, pricing, and lifecycle';

/*******************************************************************************
 * POPULATE PRODUCT DIMENSION
 *
 * Strategy:
 * --------
 * 1. Aggregate product metrics from STAGING.stg_orders:
 *    - Description: MAX(description) - most recent or longest description
 *    - Unit price: AVG(unit_price) - average price across all transactions
 *    - First sold date: MIN(invoice_date) - product introduction date
 * 2. Default category assignment: category_key = 1 (General Merchandise)
 * 3. Group by stock_code to create one row per product
 * 4. Only include valid records (is_valid = TRUE)
 * 5. Initialize SCD Type 2 columns:
 *    - _effective_from = first_sold_date (product lifecycle start)
 *    - _effective_to = NULL (currently active)
 *    - _is_current = TRUE (active version)
 *
 * Data Source:
 * -----------
 * STAGING.stg_orders contains transactional data with stock_code, description,
 * and unit_price. We aggregate to product level to create one row per product.
 *
 * Aggregation Logic:
 * -----------------
 * - MAX(description): Handles cases where description varies slightly for same
 *   stock_code. Takes the "longest" or "latest" description alphabetically.
 *   In production, you'd apply more sophisticated description deduplication.
 *
 * - AVG(unit_price): Averages all transaction prices for this product. This
 *   smooths out temporary discounts and promotional pricing to show typical price.
 *
 * - MIN(invoice_date): Identifies when product first appeared in dataset. This
 *   serves as product "launch date" for lifecycle analysis.
 *
 * Initial Load vs. Incremental Updates:
 * -------------------------------------
 * This script performs an INITIAL LOAD, creating the first version of each product.
 * Future incremental loads would:
 * 1. Detect product attribute changes (e.g., significant price change)
 * 2. Expire the old row (set _effective_to, _is_current = FALSE)
 * 3. Insert new row with updated attributes (_is_current = TRUE)
 *
 * For this portfolio project, we implement the initial load only. Incremental
 * SCD Type 2 updates would require a separate ETL script with change detection logic.
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.dim_product (
  stock_code,
  description,
  category_key,
  unit_price,
  first_sold_date,
  _effective_from,
  _effective_to,
  _is_current
)
SELECT
  stock_code,
  MAX(description) AS description,           -- Most recent/longest description
  1 AS category_key,                         -- Default: General Merchandise
  AVG(unit_price) AS unit_price,             -- Average price across transactions
  MIN(invoice_date::DATE) AS first_sold_date,
  MIN(invoice_date::DATE) AS _effective_from, -- Product became active on first sale
  NULL AS _effective_to,                      -- Currently active (no end date)
  TRUE AS _is_current                         -- Active version
FROM ECOMMERCE_DW.STAGING.stg_orders
WHERE is_valid = TRUE                         -- Only include validated records
GROUP BY stock_code
ORDER BY stock_code;

-- Confirm population
SELECT 'dim_product populated successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.dim_product;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.dim_product;

-- View sample products with all attributes
SELECT
  product_key,
  stock_code,
  description,
  category_key,
  unit_price,
  first_sold_date,
  _effective_from,
  _effective_to,
  _is_current
FROM ECOMMERCE_DW.PRODUCTION.dim_product
LIMIT 10;

-- Verify all products are currently active (initial load)
SELECT
  _is_current,
  COUNT(*) AS product_count
FROM ECOMMERCE_DW.PRODUCTION.dim_product
GROUP BY _is_current;

-- Product price distribution
SELECT
  CASE
    WHEN unit_price < 1 THEN 'Under £1'
    WHEN unit_price BETWEEN 1 AND 5 THEN '£1-£5'
    WHEN unit_price BETWEEN 5 AND 10 THEN '£5-£10'
    WHEN unit_price BETWEEN 10 AND 20 THEN '£10-£20'
    ELSE 'Over £20'
  END AS price_range,
  COUNT(*) AS product_count
FROM ECOMMERCE_DW.PRODUCTION.dim_product
WHERE _is_current = TRUE
GROUP BY price_range
ORDER BY MIN(unit_price);

-- Verify foreign key relationship with dim_category
SELECT
  c.category_name,
  COUNT(p.product_key) AS product_count
FROM ECOMMERCE_DW.PRODUCTION.dim_product p
INNER JOIN ECOMMERCE_DW.PRODUCTION.dim_category c
  ON p.category_key = c.category_key
WHERE p._is_current = TRUE
GROUP BY c.category_name
ORDER BY product_count DESC;

-- Top 10 most expensive products
SELECT
  stock_code,
  description,
  unit_price,
  first_sold_date
FROM ECOMMERCE_DW.PRODUCTION.dim_product
WHERE _is_current = TRUE
ORDER BY unit_price DESC
LIMIT 10;

-- Top 10 newest products (most recently introduced)
SELECT
  stock_code,
  description,
  unit_price,
  first_sold_date
FROM ECOMMERCE_DW.PRODUCTION.dim_product
WHERE _is_current = TRUE
ORDER BY first_sold_date DESC
LIMIT 10;

-- Check for products without descriptions
SELECT
  COUNT(*) AS products_without_description
FROM ECOMMERCE_DW.PRODUCTION.dim_product
WHERE description IS NULL OR TRIM(description) = '';

/*******************************************************************************
 * USAGE NOTES
 *
 * Joining with Fact Tables:
 * -------------------------
 * The fact_sales table will reference this dimension via product_key:
 *
 * SELECT
 *   p.stock_code,
 *   p.description,
 *   SUM(f.quantity) AS units_sold,
 *   SUM(f.total_amount) AS total_revenue
 * FROM fact_sales f
 * INNER JOIN dim_product p ON f.product_key = p.product_key
 * WHERE p._is_current = TRUE  -- Only use current product versions
 * GROUP BY p.stock_code, p.description
 * ORDER BY total_revenue DESC;
 *
 * SCD Type 2 Query Patterns:
 * -------------------------
 * 1. Current products only (most common):
 *    WHERE _is_current = TRUE
 *
 * 2. Historical point-in-time query (as of specific date):
 *    WHERE '2010-06-15' BETWEEN _effective_from AND COALESCE(_effective_to, '9999-12-31')
 *
 * 3. All versions of a specific product (audit trail):
 *    WHERE stock_code = '22423'
 *    ORDER BY _effective_from
 *
 * 4. Price change history:
 *    SELECT stock_code, unit_price, _effective_from, _effective_to
 *    FROM dim_product
 *    WHERE stock_code = '22423'
 *    ORDER BY _effective_from;
 *
 * Common Analysis Patterns:
 * ------------------------
 * 1. Product performance (revenue, units sold by product)
 * 2. Price point analysis (sales by price range)
 * 3. Product lifecycle analysis (new vs mature vs declining products)
 * 4. Category-level rollups (via category_key → dim_category)
 * 5. Product profitability (combine with cost data if available)
 *
 * Product Categorization Enhancement (Future):
 * --------------------------------------------
 * To improve category assignments, consider implementing:
 *
 * 1. Rule-Based Categorization:
 * UPDATE dim_product
 * SET category_key = CASE
 *   WHEN UPPER(description) LIKE '%GARDEN%' THEN 2        -- Home & Garden
 *   WHEN UPPER(description) LIKE '%GIFT%' THEN 3          -- Gifts & Accessories
 *   WHEN UPPER(description) LIKE '%PEN%' THEN 4           -- Office Supplies
 *   WHEN UPPER(description) LIKE '%PARTY%' THEN 5         -- Party Supplies
 *   WHEN UPPER(description) LIKE '%TOY%' THEN 6           -- Toys & Games
 *   WHEN UPPER(description) LIKE '%JEWELRY%' THEN 7       -- Fashion & Jewelry
 *   ELSE 1                                                 -- General Merchandise
 * END
 * WHERE _is_current = TRUE;
 *
 * 2. Machine Learning Classification:
 *    - Use Python/scikit-learn to train text classifier
 *    - Features: TF-IDF vectors from product descriptions
 *    - Labels: Manually categorized training set
 *    - Apply predictions to uncategorized products
 *
 * SCD Type 2 Incremental Update Pattern (Future Implementation):
 * --------------------------------------------------------------
 * When a product's price changes significantly:
 *
 * 1. Identify products with price changes > 10%:
 *    SELECT stock_code, new_price
 *    FROM staging_new_data
 *    WHERE ABS((new_price - old_price) / old_price) > 0.10;
 *
 * 2. Expire old version:
 *    UPDATE dim_product
 *    SET _effective_to = CURRENT_TIMESTAMP(), _is_current = FALSE
 *    WHERE stock_code IN (changed_products) AND _is_current = TRUE;
 *
 * 3. Insert new version with updated price:
 *    INSERT INTO dim_product (stock_code, description, unit_price, _effective_from, _is_current)
 *    VALUES ('22423', 'Product Name', new_price, CURRENT_TIMESTAMP(), TRUE);
 *
 * Performance Considerations:
 * --------------------------
 * - AUTOINCREMENT primary key enables efficient joins
 * - _is_current flag simplifies queries (no date range filters needed)
 * - Foreign key to dim_category enables category-level rollups
 * - Average price calculation avoids repeated aggregation in queries
 *
 * Future Enhancements:
 * -------------------
 * For a production implementation, consider adding:
 * - brand_name: Product brand for brand analysis
 * - supplier_id: Foreign key to supplier dimension
 * - product_cost: For margin and profitability analysis
 * - weight_kg, dimensions_cm: For shipping calculations
 * - is_active: Soft delete flag for discontinued products
 * - product_url: Link to product page on e-commerce site
 * - image_url: Product image for visual merchandising
 * - sku_variant: Size, color, or other variants
 ******************************************************************************/
