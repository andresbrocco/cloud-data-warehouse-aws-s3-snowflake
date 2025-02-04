# Data Warehouse Multi-Layer Architecture

## Overview

This project implements a **three-layer architecture** (also known as the medallion pattern) to organize data from raw ingestion through to analytics-ready tables. This approach provides clear separation of concerns, enables data quality controls at each stage, and allows for flexible reprocessing when business requirements change.

The three layers are:
- **RAW Layer** (Bronze): Unprocessed source data
- **STAGING Layer** (Silver): Cleaned and validated data
- **PRODUCTION Layer** (Gold): Optimized dimensional model

## Architectural Pattern

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW DIAGRAM                           │
└─────────────────────────────────────────────────────────────────────┘

   Source Systems              Cloud Storage           Data Warehouse
  ┌──────────────┐           ┌──────────────┐
  │              │           │              │         ┌─────────────────────────┐
  │  CSV Files   │  Upload   │   AWS S3     │  COPY   │  RAW LAYER (Bronze)     │
  │  (Kaggle)    ├──────────►│   Bucket     ├────────►│                         │
  │              │           │              │  INTO   │  • raw_transactions     │
  └──────────────┘           └──────────────┘         │  • Exact source copy    │
                                                      │  • No transformations   │
                                                      └──────────┬──────────────┘
                                                                 │
                                                                 │ SQL Transforms
                                                                 │ (Cleaning/Validation)
                                                                 ▼
                                                      ┌─────────────────────────┐
                                                      │  STAGING LAYER (Silver) │
                                                      │                         │
                                                      │  • stg_transactions     │
                                                      │  • Data quality checks  │
                                                      │  • Business rules       │
                                                      │  • Standardization      │
                                                      └──────────┬──────────────┘
                                                                 │
                                                                 │ SQL Transforms
                                                                 │ (Dimensional Modeling)
                                                                 ▼
                                                      ┌─────────────────────────┐
                                                      │  PRODUCTION LAYER (Gold)│
                                                      │                         │
                                                      │  • fact_sales           │
                                                      │  • dim_customer         │
                                                      │  • dim_product          │
                                                      │  • dim_date             │
                                                      │  • Optimized for BI     │
                                                      └─────────────────────────┘
```

## Layer Details

### RAW Layer (Bronze)

**Purpose**: Preserve the source data exactly as received, without any modifications or transformations.

**Characteristics**:
- Data is loaded from S3 using `COPY INTO` commands
- No data type conversions beyond what's necessary for storage
- No filtering, aggregation, or business logic
- Serves as the authoritative source of truth
- Enables reprocessing without re-extracting from source systems

**Example Tables**:
- `RAW.raw_transactions` - Direct copy of CSV data from S3

**When to Use**:
- Initial data landing after extraction from source systems
- Audit and compliance requirements (must preserve original data)
- Debugging and troubleshooting data quality issues
- Reprocessing scenarios when business logic changes

**Key Principle**: **Immutability**
> Once loaded, RAW data should never be modified. If source data changes, we either append new records or reload the entire table. This guarantees we can always trace back to what was originally received.

---

### STAGING Layer (Silver)

**Purpose**: Apply cleaning, validation, and standardization to make data consistent and ready for business use.

**Characteristics**:
- Data quality checks (null handling, duplicate removal, outlier detection)
- Data type standardization (dates formatted consistently, strings trimmed)
- Business logic applied (calculations, derived fields, lookups)
- Serves as the foundation for multiple downstream consumption patterns
- Still relatively normalized (not yet optimized for specific queries)

**Transformations Applied**:
- Remove duplicate records (based on business keys)
- Handle missing values (nulls, empty strings, placeholder values)
- Validate data ranges (negative prices, future dates, etc.)
- Standardize formats (country names, product codes, etc.)
- Calculate derived fields (total price = quantity × unit price)
- Filter out invalid or test records

**Example Tables**:
- `STAGING.stg_transactions` - Cleaned transaction data
- `STAGING.stg_customer_master` - Deduplicated customer records

**When to Use**:
- When you need "clean" data but not yet optimized for specific analytics
- As input to multiple downstream processes (PRODUCTION tables, ML models, exports)
- For data quality reporting and monitoring

**Key Principle**: **Idempotency**
> STAGING transformations should be repeatable. Running the same transform twice should produce the same output. This is typically achieved with `TRUNCATE` + `INSERT` or `CREATE OR REPLACE TABLE` patterns.

---

### PRODUCTION Layer (Gold)

**Purpose**: Provide analytics-ready tables optimized for business intelligence, reporting, and data science workloads.

**Characteristics**:
- Implements dimensional modeling (facts and dimensions)
- Denormalized for query performance (fewer joins needed)
- May include pre-aggregated tables for common queries
- Optimized for specific business questions and dashboards
- Serves as the interface for BI tools and analysts

**Dimensional Model Components**:

**Fact Tables** (Measures/Metrics):
- `fact_sales` - Transaction-level sales data with foreign keys to dimensions

**Dimension Tables** (Context/Attributes):
- `dim_customer` - Customer attributes (name, segment, country)
- `dim_product` - Product attributes (name, category, brand)
- `dim_date` - Date dimension for time-based analysis
- `dim_country` - Geographic hierarchy

**When to Use**:
- Building dashboards and reports
- Ad-hoc analytics queries
- Data science feature engineering
- Executive-level KPI tracking

**Key Principle**: **Query Performance**
> PRODUCTION tables are designed for fast reads, not fast writes. We optimize for the queries analysts actually run, which often means denormalization and strategic use of materialized views or aggregates.

---

## Data Flow Between Layers

### RAW → STAGING

```sql
-- Example: Load cleaned transactions into STAGING
CREATE OR REPLACE TABLE STAGING.stg_transactions AS
SELECT
    InvoiceNo,
    StockCode,
    TRIM(Description) AS Description,  -- Standardize: trim whitespace
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    UPPER(Country) AS Country,  -- Standardize: consistent casing
    (Quantity * UnitPrice) AS TotalPrice  -- Derived field
FROM RAW.raw_transactions
WHERE Quantity > 0  -- Filter: remove returns/cancellations
  AND UnitPrice > 0  -- Filter: remove invalid prices
  AND CustomerID IS NOT NULL;  -- Filter: require valid customer
```

**What's Happening**:
- Cleaning: Trimming whitespace, standardizing casing
- Validation: Filtering out invalid records
- Derivation: Calculating total price
- No joins yet - still relatively normalized

### STAGING → PRODUCTION

```sql
-- Example: Build fact table from STAGING
INSERT INTO PRODUCTION.fact_sales (
    sales_key,
    invoice_no,
    customer_key,
    product_key,
    date_key,
    quantity,
    unit_price,
    total_amount
)
SELECT
    ROW_NUMBER() OVER (ORDER BY s.InvoiceDate, s.InvoiceNo) AS sales_key,
    s.InvoiceNo,
    c.customer_key,  -- Foreign key lookup
    p.product_key,   -- Foreign key lookup
    d.date_key,      -- Foreign key lookup
    s.Quantity,
    s.UnitPrice,
    s.TotalPrice
FROM STAGING.stg_transactions s
LEFT JOIN PRODUCTION.dim_customer c ON s.CustomerID = c.customer_id
LEFT JOIN PRODUCTION.dim_product p ON s.StockCode = p.stock_code
LEFT JOIN PRODUCTION.dim_date d ON DATE(s.InvoiceDate) = d.full_date;
```

**What's Happening**:
- Joining: Connecting to dimension tables
- Surrogate keys: Using dimension keys instead of natural keys
- Fact grain: One row per transaction line item
- Ready for analytics: Optimized for star schema queries

---

## Benefits of Multi-Layer Architecture

### 1. Separation of Concerns
- **RAW**: Data engineers focus on reliable ingestion
- **STAGING**: Data quality teams validate and clean
- **PRODUCTION**: Analytics teams model for business needs

Each team can work independently without stepping on each other's toes.

### 2. Debugging and Troubleshooting
When something looks wrong in a report:
1. Check PRODUCTION layer (dimensional model issue?)
2. Check STAGING layer (cleaning logic wrong?)
3. Check RAW layer (source data problem?)

This layered approach makes it easy to isolate where issues occur.

### 3. Flexibility and Reprocessing
Business requirements change. When they do:
- Change STAGING transforms → Reprocess from RAW
- Change PRODUCTION model → Rebuild from STAGING
- No need to re-extract from source systems (RAW is the new "source")

### 4. Audit and Compliance
RAW layer provides an immutable audit trail:
- "What did the source system send us on date X?"
- "Did we receive this customer's data before GDPR deletion request?"
- "What was the original value before transformations?"

### 5. Performance Optimization
Each layer is optimized for its purpose:
- **RAW**: Fast writes (COPY INTO is optimized for bulk loading)
- **STAGING**: Balanced reads/writes (transformations run periodically)
- **PRODUCTION**: Fast reads (queries are the priority)

### 6. Multiple Consumption Patterns
STAGING becomes a shared resource:
- PRODUCTION dimensional model consumes it
- Machine learning pipelines consume it
- Data exports consume it
- Data quality dashboards consume it

One cleaned dataset, many uses.

---

## Design Principles

### 1. Immutability of RAW
Once data lands in RAW, it should never be updated or deleted (except for entire table reloads). This preserves data lineage and enables reprocessing.

### 2. Idempotency of Transformations
Running a transformation script twice should produce the same result. Use patterns like:
- `CREATE OR REPLACE TABLE`
- `TRUNCATE TABLE` + `INSERT INTO`
- `MERGE` statements with proper matching keys

### 3. Clear Layer Boundaries
Each layer has a specific responsibility:
- Don't put business logic in RAW
- Don't optimize for queries in STAGING
- Don't preserve every source field in PRODUCTION

### 4. Documentation at Every Layer
Each table should have:
- `COMMENT` metadata explaining its purpose
- Column comments describing business meaning
- Transformation logic documented in SQL scripts

### 5. Progressive Enhancement
Data quality improves as it moves through layers:
- RAW: "We received this"
- STAGING: "This is clean and valid"
- PRODUCTION: "This answers business questions"

---

## What Belongs in Each Layer?

### RAW Layer Contains:
- Tables matching source system schemas (or close to it)
- All columns from source, even if not currently used
- Invalid/duplicate records (we don't filter yet)
- No calculated fields (except what COPY INTO requires)

### STAGING Layer Contains:
- Cleaned and validated records only
- Standardized data types and formats
- Derived fields used by multiple downstream processes
- De-duplicated records based on business keys
- Records that pass data quality checks

### PRODUCTION Layer Contains:
- Dimensional model (facts and dimensions)
- Only columns needed for analytics
- Pre-aggregated tables if query performance requires
- Slowly changing dimension (SCD) logic if needed
- Surrogate keys for dimension relationships

---

## Common Patterns and Anti-Patterns

### ✅ Good Patterns

**Pattern**: Keep RAW tables simple
```sql
-- RAW: Just load it
CREATE TABLE RAW.raw_transactions (
    InvoiceNo VARCHAR,
    StockCode VARCHAR,
    Description VARCHAR,
    -- ... all source columns as-is
);
```

**Pattern**: STAGING does the heavy lifting
```sql
-- STAGING: Clean, validate, derive
CREATE OR REPLACE TABLE STAGING.stg_transactions AS
SELECT
    *,
    (Quantity * UnitPrice) AS total_price,
    CASE WHEN Quantity < 0 THEN 'RETURN' ELSE 'SALE' END AS transaction_type
FROM RAW.raw_transactions
WHERE /* validation logic */;
```

**Pattern**: PRODUCTION optimizes for queries
```sql
-- PRODUCTION: Dimensional model
CREATE TABLE PRODUCTION.fact_sales (
    sales_key NUMBER PRIMARY KEY,
    date_key NUMBER,  -- FK to dim_date
    customer_key NUMBER,  -- FK to dim_customer
    -- ... facts/measures
);
```

### ❌ Anti-Patterns to Avoid

**Anti-Pattern**: Complex transformations in RAW
```sql
-- Don't do this in RAW layer!
CREATE TABLE RAW.transactions_with_calculations AS
SELECT
    *,
    (Quantity * Price) AS total,  -- Calculation in RAW
    CASE ... END AS category  -- Business logic in RAW
FROM ...;
```
*Why avoid*: RAW should be source truth, not transformed data.

**Anti-Pattern**: Denormalization in STAGING
```sql
-- Don't do this in STAGING!
CREATE TABLE STAGING.stg_sales_with_customer AS
SELECT
    s.*,
    c.customer_name,  -- Denormalization too early
    c.customer_country
FROM staging_sales s
JOIN staging_customers c;
```
*Why avoid*: STAGING should stay normalized. Save denormalization for PRODUCTION.

**Anti-Pattern**: Keeping invalid data in STAGING
```sql
-- Don't do this!
CREATE TABLE STAGING.stg_transactions AS
SELECT * FROM RAW.raw_transactions;  -- No validation!
```
*Why avoid*: STAGING is where quality checks happen. Filter out bad data.

---

## Next Steps

1. **Create Tables**: Define table structures in each layer (see `sql/raw/`, `sql/staging/`, `sql/production/`)
2. **Implement Transforms**: Write SQL to move data between layers
3. **Add Quality Checks**: Implement validation logic in STAGING transforms
4. **Build Dimensional Model**: Create facts and dimensions in PRODUCTION
5. **Monitor and Optimize**: Track query performance and adjust as needed

For naming conventions used in each layer, see [naming-conventions.md](./naming-conventions.md).
