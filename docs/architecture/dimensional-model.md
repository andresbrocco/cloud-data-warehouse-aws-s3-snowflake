# Dimensional Model Documentation

## Overview

This document describes the dimensional model (snowflake schema) implemented in the PRODUCTION layer of the ECOMMERCE_DW data warehouse. The dimensional model transforms normalized, cleaned data from the STAGING layer into an analytics-optimized structure designed for business intelligence queries, reporting, and data analysis.

## Dimensional Modeling Approach

We implement a **snowflake schema** dimensional model, which differs from the more common star schema by normalizing dimension tables into multiple related tables. This approach reduces data redundancy and provides clearer hierarchical relationships.

### Snowflake Schema vs. Star Schema

**Star Schema:**
- Denormalized dimensions (all attributes in one table)
- Simpler queries (fewer joins)
- More data redundancy (repeated attribute values)
- Faster query performance for simple aggregations

**Snowflake Schema (Our Implementation):**
- Normalized dimensions (hierarchies split into separate tables)
- More complex queries (additional join required)
- Less data redundancy (shared attributes in separate tables)
- Better data integrity and easier maintenance

**Example of our normalization:**
```
Star Schema (denormalized):
fact_sales → dim_customer (customer_id, name, country, region, ...)

Snowflake Schema (normalized):
fact_sales → dim_customer (customer_id, name, country_key) → dim_country (country_key, country_name, region)
```

## Dimension Tables

### 1. dim_date - Date Dimension

**Purpose:** Enables time-based analysis, trending, and calendar-aware reporting.

**Type:** Type 1 SCD (no history tracking)

**Key Attributes:**
- `date_key` (INTEGER, PK): YYYYMMDD format (e.g., 20091201)
- `date` (DATE): Actual date value
- `year`, `quarter`, `month`: Calendar hierarchy
- `month_name`, `day_name`: Human-readable labels
- `is_weekend` (BOOLEAN): Weekend vs weekday flag

**Date Range:** 2009-01-01 to 2012-12-31 (1,461 days)

**Usage Pattern:**
```sql
SELECT
  d.year,
  d.month_name,
  SUM(f.total_amount) AS monthly_revenue
FROM fact_sales f
INNER JOIN dim_date d ON f.invoice_date_key = d.date_key
GROUP BY d.year, d.month_name;
```

**Design Notes:**
- Uses INTEGER date_key for efficient joins (faster than DATE joins)
- Generated using Snowflake's GENERATOR table function
- Small table (1,461 rows) means it's always cached in memory
- Static dimension (no updates needed unless date range extends)

---

### 2. dim_country - Country Dimension

**Purpose:** Enables geographic analysis and regional reporting.

**Type:** Type 1 SCD (no history tracking)

**Key Attributes:**
- `country_key` (INTEGER AUTOINCREMENT, PK): Surrogate key
- `country_name` (VARCHAR): Full country name
- `country_code` (VARCHAR): ISO 3166-1 alpha-2 code (placeholder)
- `region` (VARCHAR): Geographic grouping (Europe, Asia, Americas, Oceania)

**Normalization:** Denormalized from dim_customer (snowflake schema pattern)

**Usage Pattern:**
```sql
SELECT
  c.region,
  COUNT(DISTINCT cu.customer_key) AS customer_count
FROM dim_customer cu
INNER JOIN dim_country c ON cu.country_key = c.country_key
GROUP BY c.region;
```

**Design Notes:**
- Extracted from STAGING.stg_orders (distinct countries)
- Reduces redundancy (store "United Kingdom" once vs. thousands of times per customer)
- Region assignment uses CASE statement for simplified classification
- ~40 distinct countries from e-commerce dataset

---

### 3. dim_customer - Customer Dimension

**Purpose:** Enables customer segmentation, lifetime value analysis, and cohort studies.

**Type:** Type 2 SCD (tracks historical changes)

**Key Attributes:**
- `customer_key` (INTEGER AUTOINCREMENT, PK): Surrogate key
- `customer_id` (INTEGER): Business key from source system
- `country_key` (INTEGER, FK): Foreign key to dim_country
- `first_order_date` (DATE): Customer acquisition date
- `last_order_date` (DATE): Most recent purchase (recency)
- `total_lifetime_orders` (INTEGER): Order frequency

**SCD Type 2 Columns:**
- `_effective_from` (TIMESTAMP_NTZ): When this version became active
- `_effective_to` (TIMESTAMP_NTZ): When this version expired (NULL = current)
- `_is_current` (BOOLEAN): TRUE for active version, FALSE for historical

**Normalization:** References dim_country via country_key (snowflake schema)

**Usage Pattern:**
```sql
-- Current customers only
SELECT customer_id, total_lifetime_orders
FROM dim_customer
WHERE _is_current = TRUE;

-- Historical point-in-time query
SELECT customer_id, country_key
FROM dim_customer
WHERE '2010-06-15' BETWEEN _effective_from
  AND COALESCE(_effective_to, '9999-12-31');
```

**Design Notes:**
- ~4,000 distinct customers from dataset
- Pre-aggregated metrics support RFM analysis (Recency, Frequency, Monetary)
- SCD Type 2 tracks customer attribute changes (e.g., country moves)
- Initial load: all customers inserted with _is_current = TRUE
- Excludes guest transactions (customer_id IS NULL)

---

### 4. dim_category - Product Category Dimension

**Purpose:** Enables product hierarchy analysis and category performance reporting.

**Type:** Type 1 SCD (no history tracking)

**Key Attributes:**
- `category_key` (INTEGER AUTOINCREMENT, PK): Surrogate key
- `category_name` (VARCHAR): Human-readable category label

**Predefined Categories:**
1. General Merchandise (default, category_key = 1)
2. Home & Garden
3. Gifts & Accessories
4. Office Supplies
5. Party Supplies
6. Toys & Games
7. Fashion & Jewelry
8. Unknown

**Normalization:** Denormalized from dim_product (snowflake schema pattern)

**Usage Pattern:**
```sql
SELECT
  c.category_name,
  COUNT(DISTINCT p.product_key) AS product_count
FROM dim_product p
INNER JOIN dim_category c ON p.category_key = c.category_key
WHERE p._is_current = TRUE
GROUP BY c.category_name;
```

**Design Notes:**
- 8 predefined categories (manual insertion)
- All products initially assigned to category_key = 1 (General Merchandise)
- Production enhancement: implement ML classification or rule-based categorization
- Small static dimension (8 rows, always cached)

---

### 5. dim_product - Product Dimension

**Purpose:** Enables product analysis, pricing trends, and merchandising insights.

**Type:** Type 2 SCD (tracks historical changes)

**Key Attributes:**
- `product_key` (INTEGER AUTOINCREMENT, PK): Surrogate key
- `stock_code` (VARCHAR): Business key (product SKU)
- `description` (VARCHAR): Product name/description
- `category_key` (INTEGER, FK): Foreign key to dim_category
- `unit_price` (DECIMAL): Average unit price from transactions
- `first_sold_date` (DATE): Product launch date

**SCD Type 2 Columns:**
- `_effective_from` (TIMESTAMP_NTZ): When this version became active
- `_effective_to` (TIMESTAMP_NTZ): When this version expired (NULL = current)
- `_is_current` (BOOLEAN): TRUE for active version, FALSE for historical

**Normalization:** References dim_category via category_key (snowflake schema)

**Usage Pattern:**
```sql
-- Current products with category
SELECT
  p.stock_code,
  p.description,
  c.category_name,
  p.unit_price
FROM dim_product p
INNER JOIN dim_category c ON p.category_key = c.category_key
WHERE p._is_current = TRUE;

-- Price change history
SELECT stock_code, unit_price, _effective_from, _effective_to
FROM dim_product
WHERE stock_code = '22423'
ORDER BY _effective_from;
```

**Design Notes:**
- ~3,000-4,000 distinct products from dataset
- unit_price = AVG(unit_price) from all transactions (smooths discounts)
- description = MAX(description) for deduplication
- SCD Type 2 tracks price changes and description updates
- Initial load: all products inserted with _is_current = TRUE

---

## Slowly Changing Dimensions (SCD) Strategy

### What is SCD Type 2?

SCD Type 2 preserves full historical changes by creating new rows for each attribute change. This allows historical analysis to query data as it existed at any point in time.

### SCD Type 2 Columns

**_effective_from:**
- When this version of the record became active
- For initial load: set to first_order_date (customer) or first_sold_date (product)
- For updates: set to the timestamp of the change

**_effective_to:**
- When this version of the record expired
- NULL indicates the record is currently active
- Set when a new version is created

**_is_current:**
- Boolean flag indicating the active version
- TRUE = current/active version (most queries filter on this)
- FALSE = historical version (for historical analysis only)

### Why Use Underscore Prefix?

The underscore prefix (_effective_from, _effective_to, _is_current) indicates these are metadata columns for internal tracking, not business attributes. This is a standard naming convention in dimensional modeling.

### SCD Type 2 Query Patterns

**1. Current Records Only (Most Common):**
```sql
SELECT * FROM dim_customer WHERE _is_current = TRUE;
```

**2. Historical Point-in-Time Query:**
```sql
SELECT *
FROM dim_customer
WHERE '2010-06-15' BETWEEN _effective_from
  AND COALESCE(_effective_to, '9999-12-31');
```

**3. Full Change History:**
```sql
SELECT *
FROM dim_customer
WHERE customer_id = 12345
ORDER BY _effective_from;
```

### When to Create New SCD Type 2 Versions

**For dim_customer:**
- Customer moves to a different country
- Significant updates to customer profile attributes

**For dim_product:**
- Product price changes by more than 10%
- Product description is significantly updated
- Product category is reassigned

### SCD Type 2 Incremental Update Process

**Step 1: Detect Changes**
```sql
-- Identify customers whose country changed
SELECT customer_id, new_country
FROM staging_new_data
WHERE (customer_id, country) NOT IN (
  SELECT customer_id, country FROM dim_customer WHERE _is_current = TRUE
);
```

**Step 2: Expire Old Version**
```sql
UPDATE dim_customer
SET _effective_to = CURRENT_TIMESTAMP(), _is_current = FALSE
WHERE customer_id IN (changed_customers) AND _is_current = TRUE;
```

**Step 3: Insert New Version**
```sql
INSERT INTO dim_customer (
  customer_id, country_key, _effective_from, _is_current
)
VALUES (12345, new_country_key, CURRENT_TIMESTAMP(), TRUE);
```

---

## Foreign Key Relationships

### Snowflake Schema Hierarchies

Our dimensional model implements two normalized hierarchies:

**1. Customer → Country Hierarchy:**
```
fact_sales.customer_key → dim_customer.customer_key
dim_customer.country_key → dim_country.country_key
```

**2. Product → Category Hierarchy:**
```
fact_sales.product_key → dim_product.product_key
dim_product.category_key → dim_category.category_key
```

### Why Normalize Dimensions?

**Benefits:**
- **Reduced Redundancy:** Country name stored once, not replicated per customer
- **Consistent Attributes:** Regional groupings applied uniformly
- **Easier Updates:** Changing a country's region updates all customers automatically
- **Shared Dimensions:** Can be used by multiple fact tables

**Trade-offs:**
- **Additional Join:** Queries require one more join to access normalized attributes
- **Slightly More Complex:** Queries are more verbose (but not significantly slower in Snowflake)

### Snowflake Foreign Key Handling

**Important Note:** Snowflake does NOT enforce foreign key constraints. Foreign keys in Snowflake are informational only, used for:
- Query optimization (join elimination, predicate pushdown)
- Documentation (relationship visibility)
- BI tool metadata (automatic join suggestions)

**To ensure referential integrity:**
- Use LEFT JOIN when foreign keys may be NULL
- Validate lookups during ETL (ensure referenced keys exist)
- Implement data quality checks before loading

---

## Dimensional Model Diagram

```
                    ┌──────────────────┐
                    │   dim_country    │
                    ├──────────────────┤
                    │ country_key (PK) │
                    │ country_name     │
                    │ country_code     │
                    │ region           │
                    └────────┬─────────┘
                             │
                             │ country_key (FK)
                             │
                    ┌────────▼─────────┐
                    │  dim_customer    │
                    ├──────────────────┤
                    │ customer_key(PK) │
                    │ customer_id      │
                    │ country_key (FK) │◄────────┐
                    │ first_order_date │         │
                    │ last_order_date  │         │
                    │ total_orders     │         │
                    │ _effective_from  │         │
                    │ _effective_to    │         │
                    │ _is_current      │         │
                    └──────────────────┘         │
                                                 │
                                                 │ customer_key (FK)
                                                 │
┌──────────────────┐                   ┌────────┴─────────┐                   ┌──────────────────┐
│   dim_category   │                   │   fact_sales     │                   │    dim_date      │
├──────────────────┤                   ├──────────────────┤                   ├──────────────────┤
│ category_key(PK) │                   │ sales_key (PK)   │                   │ date_key (PK)    │
│ category_name    │                   │ date_key (FK)    ├───────────────────┤ date             │
└────────┬─────────┘                   │ customer_key(FK) │                   │ year             │
         │                             │ product_key (FK) │                   │ quarter          │
         │ category_key (FK)           │ country_key (FK) │                   │ month            │
         │                             │ invoice_no       │                   │ month_name       │
         │                             │ quantity         │                   │ day              │
         │                             │ unit_price       │                   │ day_of_week      │
         │                             │ total_amount     │                   │ day_name         │
         │                             │ _loaded_at       │                   │ is_weekend       │
         │                             └────────┬─────────┘                   └──────────────────┘
         │                                      │
         │ category_key (FK)                    │ product_key (FK)
         │                                      │
         │                             ┌────────▼─────────┐
         │                             │  dim_product     │
         │                             ├──────────────────┤
         └─────────────────────────────┤ product_key (PK) │
                                       │ stock_code       │
                                       │ description      │
                                       │ category_key(FK) │
                                       │ unit_price       │
                                       │ first_sold_date  │
                                       │ _effective_from  │
                                       │ _effective_to    │
                                       │ _is_current      │
                                       └──────────────────┘

Legend:
  PK = Primary Key
  FK = Foreign Key
  ─── = Relationship (one-to-many)
```

---

## Data Lineage

The dimensional model sources data from the STAGING layer, which in turn sources from the RAW layer:

**Full Lineage:**
```
S3 Parquet Files
  → RAW.raw_online_retail_parquet (immutable source)
    → STAGING.stg_orders (cleaned, validated)
      → PRODUCTION.dim_date (generated)
      → PRODUCTION.dim_country (extracted from stg_orders.country)
      → PRODUCTION.dim_customer (aggregated from stg_orders by customer_id)
      → PRODUCTION.dim_category (predefined categories)
      → PRODUCTION.dim_product (aggregated from stg_orders by stock_code)
      → PRODUCTION.fact_sales (transactional facts with dimension lookups)
```

**Key Transformation Points:**

1. **RAW → STAGING:**
   - Type conversions (VARCHAR → INTEGER, DECIMAL, TIMESTAMP)
   - Data validation (quality flags)
   - Business logic (total_amount calculation)

2. **STAGING → PRODUCTION (Dimensions):**
   - Aggregation (customer/product metrics)
   - Normalization (country/category extraction)
   - SCD Type 2 initialization (effective dates)
   - Foreign key lookups (country_key, category_key)

---

## Common Query Patterns

### Regional Sales Analysis
```sql
SELECT
  co.region,
  co.country_name,
  COUNT(DISTINCT cu.customer_key) AS customer_count,
  SUM(f.total_amount) AS total_revenue
FROM fact_sales f
INNER JOIN dim_customer cu ON f.customer_key = cu.customer_key
INNER JOIN dim_country co ON cu.country_key = co.country_key
WHERE cu._is_current = TRUE
GROUP BY co.region, co.country_name
ORDER BY total_revenue DESC;
```

### Category Performance
```sql
SELECT
  c.category_name,
  COUNT(DISTINCT p.product_key) AS product_count,
  SUM(f.quantity) AS units_sold,
  SUM(f.total_amount) AS total_revenue
FROM fact_sales f
INNER JOIN dim_product p ON f.product_key = p.product_key
INNER JOIN dim_category c ON p.category_key = c.category_key
WHERE p._is_current = TRUE
GROUP BY c.category_name
ORDER BY total_revenue DESC;
```

### Time-Series Analysis
```sql
SELECT
  d.year,
  d.month_name,
  SUM(f.total_amount) AS monthly_revenue,
  COUNT(DISTINCT f.customer_key) AS active_customers
FROM fact_sales f
INNER JOIN dim_date d ON f.invoice_date_key = d.date_key
GROUP BY d.year, d.month_name, d.month
ORDER BY d.year, d.month;
```

### Customer Segmentation (RFM)
```sql
SELECT
  customer_id,
  DATEDIFF(DAY, last_order_date, CURRENT_DATE()) AS recency_days,
  total_lifetime_orders AS frequency,
  SUM(f.total_amount) AS monetary_value
FROM dim_customer cu
LEFT JOIN fact_sales f ON cu.customer_key = f.customer_key
WHERE cu._is_current = TRUE
GROUP BY customer_id, last_order_date, total_lifetime_orders
ORDER BY monetary_value DESC;
```

---

## Best Practices

### Querying SCD Type 2 Dimensions

**Always filter for current records unless doing historical analysis:**
```sql
-- Good: Filter for current customers
SELECT * FROM dim_customer WHERE _is_current = TRUE;

-- Bad: Missing filter returns multiple versions per customer
SELECT * FROM dim_customer;  -- Returns historical versions too!
```

### Handling NULL Foreign Keys

**Use LEFT JOIN when foreign keys may be NULL:**
```sql
-- Handles customers without country assignments
SELECT cu.customer_id, co.country_name
FROM dim_customer cu
LEFT JOIN dim_country co ON cu.country_key = co.country_key
WHERE cu._is_current = TRUE;
```

### Efficient Date Joins

**Use integer date_key for better performance:**
```sql
-- Good: Integer join (efficient)
SELECT * FROM fact_sales f
INNER JOIN dim_date d ON f.invoice_date_key = d.date_key;

-- Avoid: Direct date column join (less efficient)
SELECT * FROM fact_sales f
INNER JOIN dim_date d ON f.invoice_date = d.date;
```

---

## Future Enhancements

### Additional Dimensions

- **dim_time:** Hour/minute/second granularity for time-of-day analysis
- **dim_supplier:** Vendor/supplier information for product sourcing
- **dim_promotion:** Marketing campaigns and promotional offers
- **dim_store:** Multi-channel support (online, retail locations)

### Dimension Enrichment

- **dim_customer:** Add email, phone, segment, acquisition source
- **dim_product:** Add brand, supplier, cost, weight, dimensions
- **dim_country:** Add ISO codes, currency, timezone, lat/long
- **dim_category:** Add parent_category_key for multi-level hierarchies

### Advanced SCD Patterns

- **Type 3 SCD:** Store previous value in separate column (e.g., previous_country)
- **Type 4 SCD:** Separate history table for dimension changes
- **Type 6 SCD:** Hybrid (combines Type 1, 2, and 3)

### Automation

- **Change Data Capture:** Automated SCD Type 2 updates from staging
- **Data Quality Monitoring:** Automated checks for dimension integrity
- **Incremental Loading:** Efficient updates for large dimension tables

---

## Fact Table

### fact_sales - Sales Transaction Fact Table

**Purpose:** Records e-commerce sales transactions at invoice line item grain

**Type:** Transaction Fact Table (additive measures)

**Grain:** One row per invoice line item (one product on one invoice)

**Key Attributes:**
- `sales_key` (INTEGER AUTOINCREMENT, PK): Surrogate key for each fact row
- `date_key` (INTEGER, FK, NOT NULL): Transaction date → dim_date
- `customer_key` (INTEGER, FK, NULLABLE): Buyer → dim_customer (NULL for guest transactions)
- `product_key` (INTEGER, FK, NOT NULL): Product sold → dim_product
- `country_key` (INTEGER, FK, NOT NULL): Shipping destination → dim_country
- `invoice_no` (VARCHAR): Degenerate dimension, groups line items into orders
- `quantity` (INTEGER): Units sold (negative = returns)
- `unit_price` (DECIMAL): Price per unit (actual transaction price)
- `total_amount` (DECIMAL): Line item total = quantity × unit_price (primary measure)
- `_loaded_at` (TIMESTAMP_NTZ): ETL timestamp

**Measures:**
- **Additive:** quantity, total_amount (can sum across all dimensions)
- **Semi-additive:** unit_price (can average, summing usually meaningless)

**Fact Table Loading:**
```sql
-- Load from staging with dimension lookups
INSERT INTO fact_sales (
  date_key, customer_key, product_key, country_key,
  invoice_no, quantity, unit_price, total_amount
)
SELECT
  stg.invoice_date_key,
  cust.customer_key,        -- LEFT JOIN (nullable for guests)
  prod.product_key,          -- INNER JOIN (required)
  country.country_key,       -- INNER JOIN (required)
  stg.invoice_no,
  stg.quantity,
  stg.unit_price,
  stg.total_amount
FROM STAGING.stg_orders stg
LEFT JOIN dim_customer cust ON stg.customer_id = cust.customer_id AND cust._is_current = TRUE
INNER JOIN dim_product prod ON stg.stock_code = prod.stock_code AND prod._is_current = TRUE
INNER JOIN dim_country country ON stg.country = country.country_name
WHERE stg.is_valid = TRUE;
```

**Usage Patterns:**
```sql
-- Total revenue by country
SELECT c.country_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_country c ON f.country_key = c.country_key
GROUP BY c.country_name;

-- Monthly revenue trend
SELECT d.year, d.month_name, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name;

-- Top products by revenue
SELECT p.description, SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE p._is_current = TRUE
GROUP BY p.description
ORDER BY revenue DESC;
```

**Design Notes:**
- **Grain:** Invoice line item enables product-level analysis and basket analysis
- **NULL customer_key:** Allowed for guest transactions (LEFT JOIN in queries)
- **SCD Type 2 lookups:** Join on `_is_current = TRUE` for current dimension versions
- **Degenerate dimension:** invoice_no stored in fact (groups line items, no dimension needed)
- **Data volume:** ~400K-500K rows (filtered from staging where is_valid = TRUE)

For detailed fact table design documentation, see [Fact Table Design](fact-table-design.md).

---

## Conclusion

This dimensional model implements a complete snowflake schema optimized for e-commerce analytics. It balances normalization (reduced redundancy) with query simplicity, providing a solid foundation for business intelligence reporting, customer analysis, and product performance tracking.

The SCD Type 2 implementation on customer and product dimensions enables historical analysis, while the normalized country and category dimensions reduce data redundancy and improve maintainability.

The fact_sales table completes the dimensional model, enabling end-to-end analytics queries across all dimensions with comprehensive business metrics.
