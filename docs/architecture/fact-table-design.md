# Fact Table Design Documentation

## Overview

This document describes the design and implementation of the `fact_sales` table, which serves as the central fact table in our snowflake schema dimensional model. The fact table contains transactional sales data with measures (metrics) and foreign keys to dimension tables, enabling comprehensive business analytics and reporting.

## Fact Table Grain

**Critical Design Decision:** Grain = One row per invoice line item

### What is Grain?

The grain of a fact table defines what each row represents. It's the most fundamental design decision because it determines:
- The level of detail available for analysis
- Which questions can be answered
- How data aggregates to higher levels

### Our Grain: Invoice Line Item

Each row in `fact_sales` represents a single product on a single invoice:
- Invoice `INV001` with 3 products → 3 rows in `fact_sales`
- Each row captures: product, quantity, price, and total for that line item
- Multiple line items share the same `invoice_no` (degenerate dimension)

### Why This Grain?

**Benefits:**
- **Product-level analysis:** Which products were purchased together?
- **Basket analysis:** What's the average number of items per order?
- **Product affinity:** What products are frequently co-purchased?
- **Maximum flexibility:** Can aggregate to order, customer, or product level

**Alternatives Considered:**
- **Order level (one row per invoice):** Loses product detail, cannot analyze line items
- **Daily summary:** Loses transaction detail, cannot trace back to individual orders
- **Product-day level:** Pre-aggregated, reduces flexibility for custom time periods

## Table Structure

### Primary Key

```sql
sales_key INTEGER AUTOINCREMENT PRIMARY KEY
```

**Purpose:** Unique identifier for each fact row

**Why Use Surrogate Key?**
- Enables efficient updates/deletes if needed
- Simplifies joins if fact is used as a dimension (e.g., returns table referencing sales)
- Consistent with dimension table design pattern
- Provides stable reference even if source data changes

### Foreign Keys (Linking to Dimensions)

Foreign keys enable "slicing and dicing" - filtering and grouping by dimension attributes.

#### date_key (INTEGER, NOT NULL)

- **Links to:** `dim_date.date_key`
- **Format:** YYYYMMDD (e.g., 20101201 for December 1, 2010)
- **Purpose:** Time-based analysis, trending, seasonality
- **NOT NULL:** Every transaction must have a date
- **Usage:** Filter by date ranges, group by month/quarter/year

```sql
-- Example: Monthly revenue trend
SELECT d.year, d.month_name, SUM(f.total_amount)
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name;
```

#### customer_key (INTEGER, NULLABLE)

- **Links to:** `dim_customer.customer_key`
- **Purpose:** Customer segmentation, lifetime value, behavior analysis
- **NULLABLE:** Some orders are guest transactions (no customer_id in source)
- **NULL Handling:** Use LEFT JOIN when querying, or filter `WHERE customer_key IS NOT NULL`

**Guest Transaction Strategy:**

We allow NULL customer_key rather than creating a fake "Unknown" customer dimension row. This approach:
- Explicitly represents guest transactions
- Avoids polluting dimension with fake data
- Requires LEFT JOIN in queries (minor complexity increase)

```sql
-- Example: Revenue by customer (including guests)
SELECT
  COALESCE(c.customer_id, 'GUEST') AS customer,
  SUM(f.total_amount) AS revenue
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY customer;
```

#### product_key (INTEGER, NOT NULL)

- **Links to:** `dim_product.product_key`
- **Purpose:** Product performance, pricing analysis, lifecycle tracking
- **NOT NULL:** Every line item must have a product
- **Through hierarchy:** `dim_product.category_key` → `dim_category` for category-level rollups

```sql
-- Example: Category revenue (snowflake schema)
SELECT cat.category_name, SUM(f.total_amount)
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_category cat ON p.category_key = cat.category_key
GROUP BY cat.category_name;
```

#### country_key (INTEGER, NOT NULL)

- **Links to:** `dim_country.country_key`
- **Purpose:** Geographic analysis, regional performance, expansion planning
- **NOT NULL:** Every order has a shipping destination
- **Through hierarchy:** `dim_country.region` for continental/regional rollups

```sql
-- Example: Regional revenue (snowflake schema)
SELECT co.region, SUM(f.total_amount)
FROM fact_sales f
JOIN dim_country co ON f.country_key = co.country_key
GROUP BY co.region;
```

### Degenerate Dimensions

Degenerate dimensions are transaction attributes that belong to the fact but don't warrant their own dimension table.

#### invoice_no (VARCHAR(50))

- **Purpose:** Order/transaction identifier, groups line items into orders
- **Not a dimension because:**
  - No descriptive attributes beyond the ID
  - One-to-many relationship with fact (multiple rows per invoice)
  - Queried as a filter, not for joining to attributes

**Usage Patterns:**
```sql
-- Count distinct orders (not just line items)
SELECT COUNT(DISTINCT invoice_no) AS order_count
FROM fact_sales;

-- Filter to specific order
SELECT * FROM fact_sales WHERE invoice_no = 'INV001';

-- Order-level aggregation
SELECT invoice_no, SUM(total_amount) AS order_total
FROM fact_sales
GROUP BY invoice_no;
```

### Measures (Numeric Business Metrics)

Measures are numeric values that can be aggregated (SUM, AVG, COUNT). Our fact table contains **additive** measures - can be summed across any dimension.

#### quantity (INTEGER)

- **Definition:** Number of units sold on this line item
- **Additive:** Can sum across products, dates, customers
- **Negative values:** Indicate returns/refunds
- **Usage:** Inventory analysis, units sold metrics, demand forecasting

#### unit_price (DECIMAL(10,2))

- **Definition:** Price per unit in GBP (transaction price)
- **Semi-additive:** Can average but summing is usually meaningless
- **Stored in fact:** Captures actual transaction price (may differ from dim_product.unit_price due to discounts)
- **Usage:** Price point analysis, discount detection, margin calculation

**Why store price in both fact and dimension?**
- **dim_product.unit_price:** Average/typical price for the product
- **fact_sales.unit_price:** Actual price paid in this transaction
- Differences reveal discounts, promotions, or price changes over time

#### total_amount (DECIMAL(12,2))

- **Definition:** Line item total = quantity × unit_price
- **Additive:** Primary revenue measure, can sum across all dimensions
- **Negative values:** Indicate refunds/returns (negative quantity)
- **Usage:** Revenue reporting, all financial analytics

**This is the most important measure** - foundation for:
- Total revenue: `SUM(total_amount)`
- Average order value: `SUM(total_amount) / COUNT(DISTINCT invoice_no)`
- Customer lifetime value: `SUM(total_amount) per customer`
- Product performance: `SUM(total_amount) per product`

### Audit Columns

#### _loaded_at (TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())

- **Purpose:** When this row was inserted into the fact table
- **Usage:** ETL monitoring, data lineage, incremental processing
- **Underscore prefix:** Indicates internal metadata column

## Measure Types Explained

### Additive Measures
Can be summed across **all** dimensions:
- `quantity`: Sum across products, dates, customers → total units sold
- `total_amount`: Sum across any dimension → total revenue

### Semi-Additive Measures
Can be summed across **some** dimensions, not all:
- Inventory balance: Can sum across products, NOT across time (snapshot)
- Account balance: Can sum across accounts, NOT across time

*Note: Our fact table has no semi-additive measures*

### Non-Additive Measures
Cannot be summed, only averaged or counted:
- `unit_price`: AVG(unit_price) is meaningful, SUM(unit_price) is not
- Percentages and ratios are typically non-additive

## ETL Process: Loading the Fact Table

### Source Data

**From:** `STAGING.stg_orders` (WHERE `is_valid = TRUE`)

**Why only valid records?**
- Staging layer flags data quality issues (`is_valid = FALSE`)
- Invalid records retained in staging for monitoring
- Production fact table contains only clean, validated data

### Dimension Lookup Strategy

The core ETL challenge is converting business keys to surrogate keys:

| Staging Column | Fact Column | Lookup Method |
|----------------|-------------|---------------|
| `invoice_date_key` | `date_key` | Direct copy (pre-computed) |
| `customer_id` | `customer_key` | LEFT JOIN to `dim_customer` |
| `stock_code` | `product_key` | INNER JOIN to `dim_product` |
| `country` | `country_key` | INNER JOIN to `dim_country` |

### Join Types

**INNER JOIN (Required relationships):**
- **dim_date:** Every transaction must have a date
- **dim_product:** Every line item must have a product
- **dim_country:** Every transaction must have a shipping destination

If dimension lookup fails, row is excluded (indicates data quality issue).

**LEFT JOIN (Optional relationships):**
- **dim_customer:** Guest transactions don't have customer_id
- NULL customer_key is valid for guest transactions

### SCD Type 2 Lookups

Customer and product dimensions use SCD Type 2 (historical tracking), meaning multiple versions may exist. When loading facts, we must specify which version:

**Current Load (most common):**
```sql
JOIN dim_customer ON stg.customer_id = dim_customer.customer_id
  AND dim_customer._is_current = TRUE
```

**Historical Load (backfilling):**
```sql
JOIN dim_customer ON stg.customer_id = dim_customer.customer_id
  AND stg.invoice_date BETWEEN dim_customer._effective_from
    AND COALESCE(dim_customer._effective_to, '9999-12-31')
```

This project uses **current load** (all data loaded at once as historical).

### ETL SQL Pattern

```sql
INSERT INTO fact_sales (
  date_key, customer_key, product_key, country_key,
  invoice_no, quantity, unit_price, total_amount
)
SELECT
  stg.invoice_date_key,
  cust.customer_key,
  prod.product_key,
  country.country_key,
  stg.invoice_no,
  stg.quantity,
  stg.unit_price,
  stg.total_amount
FROM STAGING.stg_orders stg
LEFT JOIN dim_customer cust
  ON stg.customer_id = cust.customer_id
  AND cust._is_current = TRUE
INNER JOIN dim_product prod
  ON stg.stock_code = prod.stock_code
  AND prod._is_current = TRUE
INNER JOIN dim_country country
  ON stg.country = country.country_name
WHERE stg.is_valid = TRUE;
```

### Idempotency Strategy

To make fact loading re-runnable (safe to execute multiple times):

**1. TRUNCATE before load (simplest):**
```sql
TRUNCATE TABLE fact_sales;
INSERT INTO fact_sales SELECT ...;
```

**2. DELETE then INSERT (for specific date ranges):**
```sql
DELETE FROM fact_sales WHERE date_key BETWEEN 20091201 AND 20091231;
INSERT INTO fact_sales SELECT ... WHERE date_key BETWEEN 20091201 AND 20091231;
```

**3. MERGE (upsert pattern):**
```sql
MERGE INTO fact_sales f
USING staging s ON f.invoice_no = s.invoice_no AND f.stock_code = s.stock_code
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...;
```

This project uses **TRUNCATE + INSERT** pattern (full refresh).

## Data Validation

After loading, always validate data integrity:

### 1. Row Count Validation
```sql
-- Should match staging valid record count
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM stg_orders WHERE is_valid = TRUE;
```

### 2. Revenue Total Validation
```sql
-- Totals should match exactly
SELECT SUM(total_amount) FROM fact_sales;
SELECT SUM(total_amount) FROM stg_orders WHERE is_valid = TRUE;
```

### 3. Foreign Key Integrity
```sql
-- Should return 0 (no orphaned keys)
SELECT COUNT(*) FROM fact_sales f
WHERE NOT EXISTS (SELECT 1 FROM dim_product p WHERE f.product_key = p.product_key);
```

### 4. NULL Analysis
```sql
-- Check expected NULL pattern for customer_key
SELECT
  COUNT(*) AS total_rows,
  COUNT(customer_key) AS with_customer,
  COUNT(*) - COUNT(customer_key) AS guest_transactions
FROM fact_sales;
```

### 5. Date Range Validation
```sql
-- Verify expected date range (2009-2011 for this dataset)
SELECT
  TO_DATE(MIN(date_key)::VARCHAR, 'YYYYMMDD') AS min_date,
  TO_DATE(MAX(date_key)::VARCHAR, 'YYYYMMDD') AS max_date
FROM fact_sales;
```

## Query Patterns

### Time-Series Analysis
```sql
SELECT
  d.year,
  d.quarter,
  SUM(f.total_amount) AS quarterly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
```

### Geographic Analysis
```sql
SELECT
  co.region,
  co.country_name,
  SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_country co ON f.country_key = co.country_key
GROUP BY co.region, co.country_name
ORDER BY revenue DESC;
```

### Product Performance
```sql
SELECT
  p.stock_code,
  p.description,
  cat.category_name,
  SUM(f.quantity) AS units_sold,
  SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_category cat ON p.category_key = cat.category_key
WHERE p._is_current = TRUE
GROUP BY p.stock_code, p.description, cat.category_name
ORDER BY revenue DESC;
```

### Customer Lifetime Value
```sql
SELECT
  c.customer_id,
  COUNT(DISTINCT f.invoice_no) AS order_count,
  SUM(f.total_amount) AS lifetime_value,
  AVG(f.total_amount) AS avg_line_item
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c._is_current = TRUE
GROUP BY c.customer_id
ORDER BY lifetime_value DESC;
```

## Snowflake Foreign Key Handling

**Important:** Snowflake does NOT enforce foreign key constraints at runtime.

### Why Define Foreign Keys Then?

Foreign keys in Snowflake serve as:
1. **Documentation:** Clarify relationships for developers and BI tools
2. **Query Optimization:** Enable join elimination and predicate pushdown
3. **BI Tool Metadata:** Auto-generate join suggestions in Tableau, Power BI, etc.

### Ensuring Referential Integrity

Since Snowflake doesn't enforce foreign keys, we must:
1. **Validate during ETL:** Use INNER JOIN to ensure dimension rows exist
2. **Handle NULLs explicitly:** Use LEFT JOIN for optional dimensions
3. **Monitor data quality:** Check for orphaned keys regularly
4. **Log failed lookups:** Track and investigate dimension lookup failures

## Performance Considerations

### Snowflake Optimizations

Fact tables benefit from Snowflake's automatic optimizations:

1. **Columnar Storage:** Only reads columns used in query
2. **Micro-Partitions:** Automatically partitions data into small chunks
3. **Metadata Pruning:** Skips partitions based on WHERE clause predicates
4. **Clustering Keys:** Optionally define clustering for frequently filtered columns

### Clustering Recommendation

```sql
ALTER TABLE fact_sales CLUSTER BY (date_key);
```

**Why cluster by date_key?**
- Most queries filter by date range
- Clustering improves partition pruning
- Significantly speeds up time-series queries

### Integer vs. VARCHAR Foreign Keys

Using INTEGER surrogate keys instead of VARCHAR business keys:
- **Smaller storage:** 4 bytes vs. variable length
- **Faster joins:** Integer comparison faster than string comparison
- **Better compression:** Snowflake compresses integers more efficiently

## Common Anti-Patterns to Avoid

### 1. Storing Descriptive Attributes in Fact
❌ **Bad:**
```sql
CREATE TABLE fact_sales (
  product_key INTEGER,
  product_name VARCHAR(500),  -- Don't store this in fact!
  ...
);
```

✅ **Good:**
```sql
-- Store in dimension, join when needed
SELECT f.*, p.product_name
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key;
```

### 2. Using Business Keys Instead of Surrogate Keys
❌ **Bad:**
```sql
CREATE TABLE fact_sales (
  customer_id INTEGER,  -- Business key, not surrogate
  ...
);
```

✅ **Good:**
```sql
CREATE TABLE fact_sales (
  customer_key INTEGER,  -- Surrogate key from dimension
  ...
);
```

### 3. Mixing Grains
❌ **Bad:**
```sql
-- Some rows are line items, some are order totals
INSERT INTO fact_sales ... -- Inconsistent grain!
```

✅ **Good:**
```sql
-- Consistent grain: all rows are line items
-- Aggregate to order level in queries when needed
```

### 4. Storing Pre-Aggregations
❌ **Bad:**
```sql
CREATE TABLE fact_sales (
  monthly_total DECIMAL(12,2),  -- Pre-aggregated!
  ...
);
```

✅ **Good:**
```sql
-- Store atomic transactions, aggregate in queries
SELECT SUM(total_amount) AS monthly_total
FROM fact_sales
WHERE date_key BETWEEN ... AND ...;
```

### 5. Not Validating Dimension Lookups
❌ **Bad:**
```sql
-- No validation, allows orphaned foreign keys
INSERT INTO fact_sales SELECT ...;
```

✅ **Good:**
```sql
-- Validate row counts and foreign keys after loading
-- Log and investigate failed dimension lookups
```

## Fact Table Size and Performance

### Dataset Characteristics

- **Source:** UCI Online Retail II (~1M rows in RAW layer)
- **Staging:** ~400K-500K valid rows (after filtering)
- **Fact Table:** ~400K-500K rows (one-to-one with valid staging)
- **Storage:** Minimal due to Snowflake compression (columnar, integer keys)
- **Query Performance:** Fast due to micro-partitions and pruning

### Scalability Considerations

This design scales to much larger datasets:
- **Millions of rows:** No schema changes needed
- **Billions of rows:** Consider partitioning by date, clustering
- **Real-time loads:** Implement incremental loading (MERGE pattern)
- **Historical analysis:** SCD Type 2 dimensions support point-in-time queries

## Next Steps

### Immediate Next Steps
1. ✅ Fact table created (`06_create_fact_sales.sql`)
2. ✅ Fact table loaded (`07_load_fact_sales.sql`)
3. ✅ Data validated (`08_fact_validation_queries.sql`)

### Future Enhancements
1. **Advanced Analytics:**
   - RFM (Recency, Frequency, Monetary) segmentation
   - Cohort analysis
   - Basket analysis (products purchased together)
   - Time-series forecasting

2. **Additional Fact Tables:**
   - `fact_inventory`: Stock levels over time (periodic snapshot)
   - `fact_returns`: Return transactions (linked to fact_sales)
   - `fact_customer_snapshot`: Customer metrics over time

3. **Performance Optimization:**
   - Implement clustering by date_key
   - Materialized views for common aggregations
   - Partition management for large datasets

4. **Data Quality:**
   - Automated validation tests
   - Monitoring dashboards
   - Alerting for data anomalies

5. **BI Integration:**
   - Connect Tableau/Power BI
   - Create semantic layer
   - Build executive dashboards

## Conclusion

The `fact_sales` table is the centerpiece of our dimensional model, enabling comprehensive e-commerce analytics. Its design balances:
- **Detail vs. Performance:** Line-item grain provides maximum flexibility
- **Normalization vs. Query Simplicity:** Snowflake schema reduces redundancy
- **Data Quality vs. Flexibility:** NULL handling for optional dimensions

By following dimensional modeling best practices and leveraging Snowflake's cloud data warehouse capabilities, this fact table provides a solid foundation for business intelligence, reporting, and advanced analytics.
