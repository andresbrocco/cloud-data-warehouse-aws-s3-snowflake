/*******************************************************************************
 * Script: 06_create_fact_sales.sql
 * Purpose: Create the fact table for e-commerce sales transactions
 *
 * Description:
 *   This script creates the fact_sales table, which sits at the center of the
 *   snowflake schema dimensional model. The fact table stores transactional
 *   metrics (measures) and foreign keys to dimension tables, enabling
 *   comprehensive business analytics and reporting.
 *
 *   Fact Table Grain: One row per invoice line item (order line)
 *   - Each row represents a single product on a single invoice
 *   - Invoice may have multiple line items (multiple products)
 *   - Grain is critical: defines the level of detail for all analytics
 *
 * Fact Table Design Patterns:
 *   1. Transaction Fact Table: Records business events as they occur
 *   2. Additive Measures: Can be summed across all dimensions
 *   3. Foreign Keys: Link to dimension tables for slicing/dicing
 *   4. Degenerate Dimensions: Transaction IDs that don't warrant dimension tables
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. STAGING layer populated (sql/staging/02_load_staging_from_raw.sql)
 *   3. All dimension tables created and populated:
 *      - dim_date (sql/production/01_create_dim_date.sql)
 *      - dim_country (sql/production/02_create_dim_country.sql)
 *      - dim_customer (sql/production/03_create_dim_customer.sql)
 *      - dim_category (sql/production/04_create_dim_category.sql)
 *      - dim_product (sql/production/05_create_dim_product.sql)
 *
 * Execution Instructions:
 *   1. Verify all dimension tables are populated
 *   2. Execute this entire script in Snowflake worksheet
 *   3. Verify table creation: DESC TABLE ECOMMERCE_DW.PRODUCTION.fact_sales;
 *   4. Proceed to loading script (07_load_fact_sales.sql)
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-07
 * Version: 1.0
 ******************************************************************************/

-- Set execution context
USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;
USE ROLE SYSADMIN;

/*******************************************************************************
 * FACT TABLE: fact_sales
 *
 * Design Rationale:
 * -----------------
 * The fact_sales table is a TRANSACTION FACT TABLE that records e-commerce
 * sales at the invoice line item level. This granularity enables detailed
 * analysis while supporting aggregation to higher levels (order, customer, product).
 *
 * Grain Definition (Critical):
 * ----------------------------
 * Grain = One row per invoice line item
 * - invoice_no: Identifies the order/transaction
 * - Multiple line items per invoice (different products on same order)
 * - Each line item has: product, quantity, unit price, total amount
 *
 * Why This Grain?
 * ---------------
 * - Supports product-level analysis (which products were purchased together?)
 * - Enables basket analysis and product affinity studies
 * - Allows aggregation to order level (SUM by invoice_no)
 * - Preserves maximum detail for flexible analytics
 *
 * Alternative grains considered:
 * - Order level (one row per invoice): Loses product detail, can't analyze line items
 * - Daily summary: Loses transaction detail, can't trace back to individual orders
 *
 * Foreign Keys (Linking to Dimensions):
 * -------------------------------------
 * Foreign keys enable "slicing and dicing" - filtering and grouping by dimension
 * attributes. For example:
 * - Filter by date_key to analyze specific time periods
 * - Group by customer_key to calculate customer lifetime value
 * - Filter by product_key to track specific product performance
 *
 * date_key (INTEGER, NOT NULL):
 *   - Links to dim_date for time-based analysis
 *   - YYYYMMDD format (e.g., 20101201) for efficient integer joins
 *   - Enables trending, seasonality, and time series analysis
 *   - NOT NULL because every transaction has a date
 *
 * customer_key (INTEGER, NULLABLE):
 *   - Links to dim_customer for customer segmentation and behavior analysis
 *   - NULLABLE because some orders are guest transactions (no customer_id)
 *   - Guest transactions still valuable for product/date analysis
 *   - Enables RFM analysis, cohort studies, and retention metrics
 *
 * product_key (INTEGER, NOT NULL):
 *   - Links to dim_product for product performance analysis
 *   - NOT NULL because every line item must have a product
 *   - Enables product ranking, pricing analysis, and lifecycle tracking
 *   - Through dim_product.category_key, enables category-level rollups
 *
 * country_key (INTEGER, NOT NULL):
 *   - Links to dim_country for geographic analysis
 *   - NOT NULL because every order has a shipping destination
 *   - Enables regional performance, international expansion analysis
 *   - Through dim_country.region, enables continental/regional rollups
 *
 * Degenerate Dimensions (Transaction Attributes):
 * -----------------------------------------------
 * Degenerate dimensions are attributes that belong to the transaction but
 * don't warrant their own dimension table. They're stored directly in the fact.
 *
 * invoice_no (VARCHAR(50)):
 *   - Order/transaction identifier from source system
 *   - Used to group line items into orders
 *   - Enables order-level aggregation: COUNT(DISTINCT invoice_no)
 *   - Not a dimension table because:
 *     * No descriptive attributes beyond the ID
 *     * One-to-many with fact (same invoice on multiple rows)
 *     * Queried as filter, not for joining to attributes
 *
 * Measures (Numeric Business Metrics):
 * ------------------------------------
 * Measures are numeric values that can be aggregated (SUM, AVG, COUNT).
 * Our fact table contains ADDITIVE measures - can be summed across any dimension.
 *
 * quantity (INTEGER):
 *   - Number of units sold on this line item
 *   - Additive: Can sum across products, dates, customers
 *   - Negative values indicate returns (reflected from staging)
 *   - Enables inventory analysis, units sold metrics
 *
 * unit_price (DECIMAL(10,2)):
 *   - Price per unit in GBP (source currency)
 *   - Semi-additive: Can average but summing is usually meaningless
 *   - Used for price point analysis, discount detection
 *   - Stored here (not just in dim_product) to capture actual transaction price
 *
 * total_amount (DECIMAL(12,2)):
 *   - Line item total: quantity * unit_price
 *   - Additive: Primary revenue measure, can sum across all dimensions
 *   - Negative values indicate refunds/returns
 *   - Foundation for all revenue analytics (daily sales, customer LTV, etc.)
 *
 * Audit Columns (Metadata):
 * -------------------------
 * _loaded_at (TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()):
 *   - When this row was inserted into the fact table
 *   - Enables tracking ETL pipeline execution times
 *   - Useful for incremental processing and data lineage
 *   - Underscore prefix indicates internal metadata column
 *
 * Foreign Key Constraints (Informational):
 * ----------------------------------------
 * Snowflake does NOT enforce foreign key constraints at runtime. They serve as:
 * 1. Documentation: Clarify relationships for developers and BI tools
 * 2. Query Optimization: Enable join elimination and predicate pushdown
 * 3. BI Tool Metadata: Auto-generate join suggestions in tools like Tableau
 *
 * To ensure referential integrity:
 * - Validate dimension lookups during ETL (07_load_fact_sales.sql)
 * - Use INNER JOIN for required dimensions (date, product, country)
 * - Use LEFT JOIN for optional dimensions (customer)
 * - Log or reject rows with failed lookups
 *
 * Why Use Surrogate Keys (AUTOINCREMENT)?
 * ---------------------------------------
 * sales_key is an AUTOINCREMENT surrogate key:
 * - Provides unique identifier for each fact row
 * - Enables efficient updates/deletes if needed (though rare in facts)
 * - Simplifies joins if fact is used as dimension (e.g., returns referencing sales)
 * - Consistent with dimension table design pattern
 *
 * Performance Considerations:
 * --------------------------
 * - INTEGER foreign keys: Smaller than VARCHAR, faster joins
 * - DECIMAL measures: Precise for financial calculations (avoid FLOAT)
 * - No explicit indexes: Snowflake uses micro-partitions and metadata pruning
 * - Table will auto-cluster based on query patterns
 * - date_key often used in WHERE clauses, good clustering candidate
 *
 * Fact Table Size Estimation:
 * ---------------------------
 * Dataset: UCI Online Retail II (~1M rows in staging)
 * Expected fact_sales rows: ~400K-500K (after filtering invalid records)
 * Storage: Minimal due to Snowflake compression
 * Query Performance: Fast due to columnar storage and pruning
 *
 * Common Analytics Enabled by This Fact Table:
 * --------------------------------------------
 * - Revenue by time period (daily, weekly, monthly, yearly)
 * - Top products by revenue or units sold
 * - Customer lifetime value (LTV) and purchase frequency
 * - Geographic sales distribution and regional performance
 * - Average order value and basket size
 * - Product affinity analysis (what's purchased together)
 * - Cohort analysis (customer behavior over time)
 * - Seasonal trends and demand forecasting
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.fact_sales (
  -- Primary Key (Surrogate)
  sales_key INTEGER AUTOINCREMENT PRIMARY KEY
    COMMENT 'Surrogate key: unique identifier for each sales line item',

  -- Foreign Keys (Links to Dimensions)
  date_key INTEGER NOT NULL
    COMMENT 'FK to dim_date: transaction date for time-based analysis',

  customer_key INTEGER
    COMMENT 'FK to dim_customer: buyer (NULL for guest transactions)',

  product_key INTEGER NOT NULL
    COMMENT 'FK to dim_product: product sold on this line item',

  country_key INTEGER NOT NULL
    COMMENT 'FK to dim_country: shipping destination for geographic analysis',

  -- Degenerate Dimensions (Transaction Attributes)
  invoice_no VARCHAR(50)
    COMMENT 'Order/transaction identifier: groups line items into orders',

  -- Measures (Numeric Business Metrics)
  quantity INTEGER
    COMMENT 'Units sold (negative = returns)',

  unit_price DECIMAL(10,2)
    COMMENT 'Price per unit in GBP (transaction price, not dimension price)',

  total_amount DECIMAL(12,2)
    COMMENT 'Line item total: quantity * unit_price (primary revenue measure)',

  -- Audit Columns (Metadata)
  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    COMMENT 'ETL timestamp: when this row was inserted into fact table',

  -- Foreign Key Constraints (Informational, not enforced in Snowflake)
  FOREIGN KEY (date_key) REFERENCES ECOMMERCE_DW.PRODUCTION.dim_date(date_key),
  FOREIGN KEY (customer_key) REFERENCES ECOMMERCE_DW.PRODUCTION.dim_customer(customer_key),
  FOREIGN KEY (product_key) REFERENCES ECOMMERCE_DW.PRODUCTION.dim_product(product_key),
  FOREIGN KEY (country_key) REFERENCES ECOMMERCE_DW.PRODUCTION.dim_country(country_key)
)
COMMENT = 'Fact table: e-commerce sales transactions at line item grain (one row per invoice line)';

-- Confirm table creation
SELECT 'fact_sales table created successfully' AS status;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.fact_sales;

-- Display table metadata
SHOW TABLES LIKE 'fact_sales' IN SCHEMA ECOMMERCE_DW.PRODUCTION;

-- Verify table is empty (before data load)
SELECT COUNT(*) AS row_count
FROM ECOMMERCE_DW.PRODUCTION.fact_sales;

/*******************************************************************************
 * FACT TABLE DESIGN NOTES
 *
 * Star Schema vs. Snowflake Schema:
 * --------------------------------
 * This fact table is the center of a SNOWFLAKE SCHEMA:
 *
 *   fact_sales → dim_customer → dim_country
 *   fact_sales → dim_product → dim_category
 *   fact_sales → dim_date
 *
 * In a STAR SCHEMA, dimensions would be denormalized (no country/category tables).
 * Our snowflake schema reduces redundancy by normalizing hierarchies.
 *
 * Measure Types Explained:
 * ------------------------
 * ADDITIVE: Can be summed across all dimensions
 *   - quantity: SUM(quantity) across any dimension is meaningful
 *   - total_amount: SUM(total_amount) = total revenue
 *
 * SEMI-ADDITIVE: Can be summed across some dimensions, not all
 *   - Inventory balance: Can sum across products, NOT across time
 *   - Account balance: Can sum across accounts, NOT across time
 *   (Our fact has no semi-additive measures)
 *
 * NON-ADDITIVE: Cannot be summed, only averaged or counted
 *   - unit_price: AVG(unit_price) is meaningful, SUM(unit_price) is not
 *   - Percentages and ratios are typically non-additive
 *
 * Handling NULL Customer Keys:
 * ----------------------------
 * Some orders lack customer_id (guest checkouts). Two approaches:
 *
 * 1. Allow NULL (our approach):
 *    - customer_key column is NULLABLE
 *    - Use LEFT JOIN in queries: FROM fact_sales f LEFT JOIN dim_customer c
 *    - NULL indicates guest transaction
 *    - Pros: Explicit representation, no fake data
 *    - Cons: Requires LEFT JOIN, NULL handling in queries
 *
 * 2. Use "Unknown" surrogate (alternative):
 *    - Create dim_customer row with customer_key = -1, customer_id = NULL
 *    - Set fact_sales.customer_key = -1 for guest transactions
 *    - Use INNER JOIN in queries: FROM fact_sales f JOIN dim_customer c
 *    - Pros: No NULLs, simpler queries
 *    - Cons: Fake dimension row, less explicit
 *
 * Fact Table Loading Strategy:
 * ----------------------------
 * The fact table is loaded from STAGING.stg_orders via dimension lookups:
 *
 * 1. Start with valid staging records: WHERE is_valid = TRUE
 * 2. Lookup date_key: JOIN dim_date ON stg.invoice_date_key = dim_date.date_key
 * 3. Lookup customer_key: LEFT JOIN dim_customer ON stg.customer_id = dim_customer.customer_id
 * 4. Lookup product_key: JOIN dim_product ON stg.stock_code = dim_product.stock_code
 * 5. Lookup country_key: JOIN dim_country ON stg.country = dim_country.country_name
 * 6. Insert into fact_sales
 *
 * SCD Type 2 Lookups (Critical):
 * ------------------------------
 * Customer and product dimensions use SCD Type 2, which means multiple versions
 * of the same customer/product may exist. When loading facts, we MUST join on:
 *
 *   AND dim_customer._is_current = TRUE
 *   AND dim_product._is_current = TRUE
 *
 * This ensures we get the CURRENT version of the dimension. For historical
 * loads (backfilling), you'd use point-in-time logic:
 *
 *   AND transaction_date BETWEEN _effective_from AND COALESCE(_effective_to, '9999-12-31')
 *
 * Idempotency Strategy:
 * ---------------------
 * To make fact loading re-runnable (idempotent):
 *
 * 1. TRUNCATE before load (simplest, for initial load):
 *    TRUNCATE TABLE fact_sales;
 *    INSERT INTO fact_sales SELECT ...;
 *
 * 2. DELETE then INSERT (for specific date ranges):
 *    DELETE FROM fact_sales WHERE date_key BETWEEN 20091201 AND 20091231;
 *    INSERT INTO fact_sales SELECT ... WHERE date_key BETWEEN 20091201 AND 20091231;
 *
 * 3. MERGE (upsert pattern, for incremental loads):
 *    MERGE INTO fact_sales f
 *    USING staging s ON f.invoice_no = s.invoice_no AND f.stock_code = s.stock_code
 *    WHEN MATCHED THEN UPDATE ...
 *    WHEN NOT MATCHED THEN INSERT ...;
 *
 * For this project, we use TRUNCATE + INSERT pattern (full refresh).
 *
 * Query Performance Optimization:
 * ------------------------------
 * Fact tables are typically the largest tables in a data warehouse. Snowflake
 * optimizes queries using:
 *
 * 1. Columnar Storage: Only reads columns used in query
 * 2. Micro-Partitions: Automatically partitions data into small chunks
 * 3. Metadata Pruning: Skips partitions based on WHERE clause predicates
 * 4. Clustering Keys: Optionally define clustering for frequently filtered columns
 *
 * Clustering recommendation for this fact table:
 *   ALTER TABLE fact_sales CLUSTER BY (date_key);
 *
 * This improves query performance when filtering by date (most common pattern).
 *
 * Data Validation After Loading:
 * ------------------------------
 * After loading fact_sales, always validate:
 *
 * 1. Row count matches staging:
 *    - SELECT COUNT(*) FROM fact_sales vs. SELECT COUNT(*) FROM stg_orders WHERE is_valid = TRUE
 *
 * 2. Revenue totals match staging:
 *    - SELECT SUM(total_amount) FROM fact_sales vs. SELECT SUM(total_amount) FROM stg_orders
 *
 * 3. No orphaned foreign keys:
 *    - Check for date_key not in dim_date
 *    - Check for product_key not in dim_product
 *    - Check for country_key not in dim_country
 *
 * 4. Date range is expected:
 *    - SELECT MIN(date_key), MAX(date_key) FROM fact_sales
 *
 * 5. NULL analysis:
 *    - Count NULL customer_key (guest transactions) vs. expected
 *
 * Common Fact Table Anti-Patterns to Avoid:
 * -----------------------------------------
 * 1. Storing descriptive attributes in fact (e.g., product_name, customer_name)
 *    → Use dimensions for attributes, facts for measures
 *
 * 2. Using business keys instead of surrogate keys (e.g., customer_id instead of customer_key)
 *    → Breaks when business keys change, complicates SCD Type 2
 *
 * 3. Mixing grains (e.g., some rows are order-level, some are line-item level)
 *    → Results in incorrect aggregations, confusing analytics
 *
 * 4. Storing aggregations in fact (e.g., monthly_total pre-calculated)
 *    → Violates grain, complicates incremental loads, reduces flexibility
 *
 * 5. Not validating dimension lookups before loading
 *    → Results in orphaned foreign keys, broken joins, incorrect analytics
 *
 * Next Steps:
 * ----------
 * 1. Load fact table: sql/production/07_load_fact_sales.sql
 * 2. Validate fact data: sql/production/08_fact_validation_queries.sql
 * 3. Create analytics queries: sql/analytics/*.sql (future implementation)
 ******************************************************************************/
