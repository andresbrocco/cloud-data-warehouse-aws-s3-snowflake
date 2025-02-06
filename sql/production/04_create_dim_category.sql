/*******************************************************************************
 * Script: 04_create_dim_category.sql
 * Purpose: Create and populate the product category dimension table
 *
 * Description:
 *   This script creates a product category dimension table with predefined
 *   categories for organizing products into logical groupings. The category
 *   dimension enables product hierarchy analysis, category performance reporting,
 *   and merchandising insights.
 *
 *   This dimension is denormalized from the product dimension (snowflake schema
 *   pattern) to enable product categorization and hierarchical rollups.
 *
 * Key Features:
 *   - AUTOINCREMENT primary key (category_key) for surrogate key generation
 *   - category_name: Human-readable category labels
 *   - Predefined categories based on e-commerce product taxonomy
 *   - Type 1 SCD: No history tracking (category definitions are stable)
 *
 * Data Flow:
 *   Manual insert (predefined categories) → PRODUCTION.dim_category
 *   → PRODUCTION.dim_product.category_key (FK)
 *
 * Prerequisites:
 *   1. Database and schemas created (sql/setup/02_create_database_schemas.sql)
 *   2. PRODUCTION schema exists
 *
 * Execution Instructions:
 *   1. Execute this entire script in Snowflake worksheet
 *   2. Verify table creation: SELECT * FROM ECOMMERCE_DW.PRODUCTION.dim_category;
 *   3. Expected result: 8 predefined product categories
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
 * CATEGORY DIMENSION TABLE
 *
 * Design Rationale:
 * -----------------
 * The category dimension is normalized out of the product dimension following
 * the snowflake schema pattern. This enables:
 * 1. Consistent product categorization across all products
 * 2. Category-level rollup analysis (total sales by category)
 * 3. Product hierarchy navigation (drill down from category to product)
 * 4. Simplified product management (change category, all products update)
 *
 * Why Use Predefined Categories?
 * ------------------------------
 * In this e-commerce dataset, products don't come with explicit categories.
 * We define categories manually based on typical retail product groupings:
 *
 * 1. General Merchandise - Mixed products, uncategorized items
 * 2. Home & Garden - Home decor, gardening supplies
 * 3. Gifts & Accessories - Gift items, novelties, accessories
 * 4. Office Supplies - Stationery, office equipment
 * 5. Party Supplies - Party decorations, event supplies
 * 6. Toys & Games - Children's toys, games
 * 7. Fashion & Jewelry - Clothing, jewelry, fashion accessories
 * 8. Unknown - Unclassified products
 *
 * In production, products would be categorized using:
 * - Machine learning classification based on product descriptions
 * - Manual product master data management
 * - Integration with external product taxonomy services
 * - NLP analysis of product descriptions
 *
 * For this portfolio project, we create the category structure as a foundation.
 * Product-to-category mapping will be done in the dim_product script with
 * a default assignment (all products initially assigned to "General Merchandise").
 *
 * Snowflake Schema Pattern:
 * ------------------------
 * This dimension demonstrates snowflake schema normalization:
 *
 *   fact_sales → dim_product → dim_category
 *
 * The product dimension references dim_category via category_key foreign key,
 * normalizing category attributes to reduce redundancy and enable easy updates.
 *
 * Why AUTOINCREMENT for category_key?
 * -----------------------------------
 * - Surrogate key: Independent of business data changes
 * - Simplicity: Snowflake automatically generates sequential integers
 * - Performance: Integer keys are efficient for joins
 * - Flexibility: Can add categories without worrying about key collisions
 *
 * Type 1 SCD Approach:
 * -------------------
 * This dimension uses Type 1 SCD (overwrite on change) because:
 * - Category definitions are relatively stable
 * - Category name changes are rare and not historically significant
 * - Historical category analysis is not a key business requirement
 *
 * If tracking category hierarchy changes was important (e.g., "Home & Garden"
 * splits into "Home Decor" and "Garden Supplies"), you would implement Type 2 SCD.
 ******************************************************************************/

CREATE OR REPLACE TABLE ECOMMERCE_DW.PRODUCTION.dim_category (
  -- Primary Key (Surrogate)
  category_key        INTEGER AUTOINCREMENT PRIMARY KEY,

  -- Category Attributes
  category_name       VARCHAR(100) NOT NULL UNIQUE
)
COMMENT = 'Product category dimension: hierarchical product groupings for merchandising analysis';

/*******************************************************************************
 * POPULATE CATEGORY DIMENSION
 *
 * Strategy:
 * --------
 * Insert predefined categories manually. These represent a typical retail
 * product taxonomy suitable for the e-commerce dataset.
 *
 * Category Definitions:
 * --------------------
 * 1. General Merchandise (category_key = 1, used as default)
 *    - Catch-all for uncategorized or mixed products
 *    - Default assignment for products without specific classification
 *
 * 2. Home & Garden
 *    - Home decor items (frames, clocks, wall art)
 *    - Garden supplies (planters, outdoor decor)
 *
 * 3. Gifts & Accessories
 *    - Gift items (gift bags, wrapping, cards)
 *    - Small accessories and novelties
 *
 * 4. Office Supplies
 *    - Stationery (pens, notebooks, paper)
 *    - Office organization products
 *
 * 5. Party Supplies
 *    - Party decorations (balloons, banners)
 *    - Event supplies (napkins, plates, cups)
 *
 * 6. Toys & Games
 *    - Children's toys
 *    - Games and puzzles
 *
 * 7. Fashion & Jewelry
 *    - Jewelry and fashion accessories
 *    - Clothing items
 *
 * 8. Unknown
 *    - Products that cannot be classified
 *    - Placeholder for edge cases
 *
 * Extensibility:
 * -------------
 * Additional categories can be added as needed:
 * INSERT INTO dim_category (category_name) VALUES ('Electronics');
 *
 * The AUTOINCREMENT key will automatically assign the next sequential ID.
 ******************************************************************************/

INSERT INTO ECOMMERCE_DW.PRODUCTION.dim_category (category_name)
VALUES
  ('General Merchandise'),    -- Default category, category_key = 1
  ('Home & Garden'),
  ('Gifts & Accessories'),
  ('Office Supplies'),
  ('Party Supplies'),
  ('Toys & Games'),
  ('Fashion & Jewelry'),
  ('Unknown');                -- Fallback for unclassifiable products

-- Confirm population
SELECT 'dim_category populated successfully with ' || COUNT(*) || ' rows' AS status
FROM ECOMMERCE_DW.PRODUCTION.dim_category;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display table structure
DESC TABLE ECOMMERCE_DW.PRODUCTION.dim_category;

-- View all categories
SELECT
  category_key,
  category_name
FROM ECOMMERCE_DW.PRODUCTION.dim_category
ORDER BY category_key;

-- Verify General Merchandise is category_key = 1 (used as default in dim_product)
SELECT *
FROM ECOMMERCE_DW.PRODUCTION.dim_category
WHERE category_name = 'General Merchandise';

-- Count total categories
SELECT COUNT(*) AS total_categories
FROM ECOMMERCE_DW.PRODUCTION.dim_category;

/*******************************************************************************
 * USAGE NOTES
 *
 * Joining with Product Dimension:
 * -------------------------------
 * The product dimension will reference this table via category_key:
 *
 * CREATE TABLE dim_product (
 *   product_key INTEGER PRIMARY KEY,
 *   stock_code VARCHAR(50),
 *   description VARCHAR(500),
 *   category_key INTEGER,  -- Foreign key to dim_category
 *   ...
 * );
 *
 * Example join for category-level sales analysis:
 * SELECT
 *   c.category_name,
 *   COUNT(DISTINCT p.product_key) AS product_count,
 *   SUM(f.total_amount) AS total_revenue
 * FROM fact_sales f
 * INNER JOIN dim_product p ON f.product_key = p.product_key
 * INNER JOIN dim_category c ON p.category_key = c.category_key
 * GROUP BY c.category_name
 * ORDER BY total_revenue DESC;
 *
 * Common Analysis Patterns:
 * ------------------------
 * 1. Category performance comparison (revenue, units, margin by category)
 * 2. Category mix analysis (percentage of sales by category)
 * 3. Category trends over time (growth, seasonality)
 * 4. Cross-category purchasing patterns (market basket analysis)
 * 5. Category-level inventory and stock management
 *
 * Snowflake Schema Benefits:
 * -------------------------
 * - Reduced storage: Category name stored once, not replicated per product
 * - Consistent categorization: All products reference same category definitions
 * - Easier updates: Renaming a category updates all products automatically
 * - Hierarchical analysis: Drill down from category to product
 *
 * Future Product Categorization Approaches:
 * -----------------------------------------
 * For production implementation, consider:
 *
 * 1. Machine Learning Classification:
 *    - Train text classifier on product descriptions
 *    - Use libraries like scikit-learn or spaCy
 *    - Assign categories based on description keywords
 *
 * 2. Rule-Based Classification:
 *    - Use CASE statements with keyword matching
 *    - Example: WHERE description LIKE '%GARDEN%' → 'Home & Garden'
 *
 * 3. External Product Taxonomy:
 *    - Integrate with Google Product Taxonomy
 *    - Use standardized GS1 product classifications
 *
 * 4. Manual Product Master Data:
 *    - Maintain product-to-category mapping table
 *    - Regular data stewardship by merchandising team
 *
 * 5. Hierarchical Categories (Multi-Level):
 *    - Add parent_category_key for category hierarchies
 *    - Example: "Home Decor" → "Home & Garden" → "Retail"
 *
 * Example Rule-Based Categorization (for reference):
 * --------------------------------------------------
 * UPDATE dim_product
 * SET category_key = CASE
 *   WHEN UPPER(description) LIKE '%GARDEN%' OR UPPER(description) LIKE '%PLANT%' THEN 2
 *   WHEN UPPER(description) LIKE '%GIFT%' OR UPPER(description) LIKE '%BAG%' THEN 3
 *   WHEN UPPER(description) LIKE '%PEN%' OR UPPER(description) LIKE '%PAPER%' THEN 4
 *   WHEN UPPER(description) LIKE '%PARTY%' OR UPPER(description) LIKE '%BALLOON%' THEN 5
 *   WHEN UPPER(description) LIKE '%TOY%' OR UPPER(description) LIKE '%GAME%' THEN 6
 *   WHEN UPPER(description) LIKE '%JEWELRY%' OR UPPER(description) LIKE '%NECKLACE%' THEN 7
 *   ELSE 1  -- Default to General Merchandise
 * END;
 *
 * Performance Considerations:
 * --------------------------
 * - Very small dimension (8 rows) means it's always cached in memory
 * - UNIQUE constraint on category_name prevents duplicates
 * - INTEGER primary key enables efficient joins with dim_product
 * - AUTOINCREMENT simplifies key management
 *
 * Future Enhancements:
 * -------------------
 * For a production implementation, consider adding:
 * - parent_category_key: For multi-level category hierarchies
 * - category_description: Detailed category definitions
 * - display_order: For UI sorting and navigation
 * - is_active: To soft-delete obsolete categories
 * - category_manager: Responsible person for category merchandising
 * - target_margin_pct: Category-specific margin targets
 ******************************************************************************/
