# Naming Conventions

## Overview

Consistent naming conventions are critical for maintainability, collaboration, and understanding data lineage in a data warehouse. This document defines the naming standards used throughout the ECOMMERCE_DW project.

**Core Principle**: Names should be **self-documenting**. A developer should be able to understand what an object contains and which layer it belongs to just by reading its name.

---

## Database and Schema Naming

### Database Names

**Convention**: `UPPERCASE_WITH_UNDERSCORES`

**Format**: `{DOMAIN}_{PURPOSE}`

**Examples**:
- `ECOMMERCE_DW` - E-commerce data warehouse (this project)
- `FINANCE_DW` - Financial data warehouse (hypothetical)
- `MARKETING_ANALYTICS` - Marketing analytics database (hypothetical)

**Rationale**:
- Uppercase makes databases stand out in SQL scripts
- Underscores improve readability over spaces
- Suffix `_DW` clearly indicates this is a data warehouse, not an operational database

---

### Schema Names

**Convention**: `UPPERCASE`, representing layer purpose

**Format**: `{LAYER_NAME}`

**Our Schemas**:
```
ECOMMERCE_DW
├── RAW          # Bronze layer - source data as-is
├── STAGING      # Silver layer - cleaned data
└── PRODUCTION   # Gold layer - dimensional model
```

**Rationale**:
- Simple, clear layer names without unnecessary prefixes
- Uppercase distinguishes schemas from tables in SQL
- Aligned with industry-standard medallion architecture terminology

**Alternative Patterns** (not used here, but common in industry):
- `RAW_LAYER`, `STAGING_LAYER`, `PRODUCTION_LAYER` - More explicit but verbose
- `BRONZE`, `SILVER`, `GOLD` - Medallion pattern terminology
- `L1_RAW`, `L2_STAGING`, `L3_PRODUCTION` - Numbered layers

We chose simplicity: `RAW`, `STAGING`, `PRODUCTION`.

---

## Table Naming

Table names vary by layer to indicate their purpose and data quality level.

### RAW Layer Tables

**Convention**: `lowercase_with_underscores`, optionally prefixed with `raw_`

**Format**:
- `{source_name}` (if source is obvious)
- `raw_{source_name}` (if ambiguity exists)

**Examples**:
- `raw_transactions` - Raw transaction data from source
- `raw_customers` - Raw customer master data
- `raw_products` - Raw product catalog

**Rationale**:
- Lowercase indicates "raw, unprocessed data"
- `raw_` prefix makes it immediately clear this is unprocessed source data
- Matches source system naming where possible for traceability

**When to Use `raw_` Prefix**:
- When the same table name exists in multiple layers (e.g., `raw_transactions`, `stg_transactions`)
- When loading from multiple sources (e.g., `raw_erp_orders`, `raw_web_orders`)

**When to Omit `raw_` Prefix**:
- When table name is unique enough to be unambiguous
- When following source system naming exactly aids documentation

---

### STAGING Layer Tables

**Convention**: `lowercase_with_underscores`, prefixed with `stg_`

**Format**: `stg_{entity_name}`

**Examples**:
- `stg_transactions` - Cleaned transaction data
- `stg_customers` - Validated and deduplicated customers
- `stg_products` - Standardized product catalog
- `stg_customer_aggregates` - Aggregated customer metrics (if needed)

**Rationale**:
- `stg_` prefix immediately identifies this as cleaned, validated data
- Lowercase indicates "work in progress" (not final business-facing structure)
- Entity names describe what the data represents, not how it's sourced

**Naming Tips**:
- Use singular form: `stg_customer` (not `stg_customers`) - this is debatable, but singular emphasizes "each row is one customer"
- Be specific: `stg_transaction_line_items` is better than `stg_data`
- Avoid abbreviations unless universally understood: `stg_cust` ❌, `stg_customer` ✅

**Counter-Example**:
We use `stg_transactions` (plural) in this project because it's more natural for transaction data. The singular vs. plural debate is style-dependent; **consistency matters more than the choice**.

---

### PRODUCTION Layer Tables

**Convention**: `lowercase_with_underscores`, prefixed by table type

**Format**:
- **Fact Tables**: `fact_{business_process}`
- **Dimension Tables**: `dim_{entity}`
- **Aggregate Tables**: `agg_{metric}` or `summary_{metric}`
- **Bridge Tables**: `bridge_{relationship}`

**Examples**:

**Fact Tables** (measures/metrics):
- `fact_sales` - Sales transaction facts
- `fact_inventory` - Inventory snapshot facts
- `fact_orders` - Order facts

**Dimension Tables** (context/attributes):
- `dim_customer` - Customer dimension
- `dim_product` - Product dimension
- `dim_date` - Date dimension
- `dim_country` - Country/geography dimension
- `dim_brand` - Brand dimension
- `dim_category` - Product category dimension

**Aggregate Tables** (pre-computed summaries):
- `agg_monthly_sales` - Monthly sales aggregates
- `agg_customer_lifetime_value` - Customer CLV summary
- `summary_daily_metrics` - Daily KPI summary

**Bridge Tables** (many-to-many relationships):
- `bridge_product_category` - Products to categories (if many-to-many)
- `bridge_customer_segment` - Customers to segments

**Rationale**:
- Prefixes immediately indicate table type and usage pattern
- Business-friendly names (`sales`, not `transactions_fact_tbl`)
- Aligns with Kimball dimensional modeling methodology
- Makes BI tool metadata more readable

---

## Column Naming

### General Rules

**Convention**: `lowercase_with_underscores`

**Format**: `{descriptor}_{attribute}`

**Examples**:
- `customer_id` - Customer identifier
- `order_date` - Date of order
- `total_amount` - Total dollar amount
- `product_name` - Name of product
- `is_active` - Boolean flag for active status
- `created_at` - Timestamp of record creation

**Rationale**:
- Lowercase is easier to type and read in long SQL queries
- Underscores separate words clearly (better than camelCase in SQL)
- Descriptive names reduce need for comments

---

### Key Columns

**Primary Keys**:
- **RAW Layer**: Use source system key name (e.g., `InvoiceNo`, `CustomerID`)
- **STAGING Layer**: Use source system key name or standardize (e.g., `customer_id`, `invoice_no`)
- **PRODUCTION Layer**: Use surrogate keys with `_key` suffix (e.g., `customer_key`, `product_key`, `sales_key`)

**Format**:
- Natural keys: `{entity}_id` (e.g., `customer_id`)
- Surrogate keys: `{entity}_key` (e.g., `customer_key`)

**Examples**:
```sql
-- RAW layer: preserve source naming
CREATE TABLE RAW.raw_transactions (
    InvoiceNo VARCHAR,  -- Source system uses mixed case
    CustomerID NUMBER,  -- Source system name
    ...
);

-- STAGING layer: standardize to lowercase
CREATE TABLE STAGING.stg_transactions (
    invoice_no VARCHAR,  -- Standardized
    customer_id NUMBER,  -- Standardized
    ...
);

-- PRODUCTION layer: surrogate keys for dimensions
CREATE TABLE PRODUCTION.dim_customer (
    customer_key NUMBER PRIMARY KEY,  -- Surrogate key
    customer_id NUMBER,  -- Natural key from source
    ...
);

CREATE TABLE PRODUCTION.fact_sales (
    sales_key NUMBER PRIMARY KEY,  -- Fact surrogate key
    customer_key NUMBER,  -- Foreign key to dim_customer
    product_key NUMBER,   -- Foreign key to dim_product
    date_key NUMBER,      -- Foreign key to dim_date
    ...
);
```

**Rationale**:
- `_id` suffix indicates natural/business key from source system
- `_key` suffix indicates surrogate key generated by warehouse
- This distinction makes join relationships crystal clear

---

### Foreign Keys

**Convention**: Match the referenced table's primary key name

**Format**: `{referenced_table}_{key_type}`

**Examples**:
```sql
CREATE TABLE PRODUCTION.fact_sales (
    sales_key NUMBER PRIMARY KEY,
    customer_key NUMBER,  -- References dim_customer.customer_key
    product_key NUMBER,   -- References dim_product.product_key
    date_key NUMBER,      -- References dim_date.date_key
    ...
);
```

**Rationale**:
- Makes relationships obvious without needing to check schema
- Tools can auto-detect relationships based on name matching
- Reduces ambiguity in multi-table joins

---

### Boolean Columns

**Convention**: Prefix with `is_`, `has_`, or `can_`

**Examples**:
- `is_active` - Is this record active?
- `is_deleted` - Is this record soft-deleted?
- `has_discount` - Does this transaction have a discount?
- `can_refund` - Can this order be refunded?

**Rationale**:
- Makes boolean nature immediately obvious
- Reads like natural language: "WHERE is_active = TRUE"
- Prevents confusion with string/enum columns

---

### Date and Timestamp Columns

**Convention**:
- Dates: `{descriptor}_date`
- Timestamps: `{descriptor}_at` or `{descriptor}_timestamp`

**Examples**:
- `order_date` - Date portion only (e.g., '2025-02-02')
- `created_at` - Full timestamp with time (e.g., '2025-02-02 14:30:00')
- `updated_at` - Timestamp of last update
- `shipped_date` - Date item was shipped
- `invoice_timestamp` - Full timestamp of invoice generation

**Rationale**:
- `_date` vs. `_at` clarifies precision (day vs. second)
- `_at` follows Rails/common web framework conventions
- Consistent suffixes enable pattern-based queries: `SELECT * FROM table WHERE *_date = '2025-02-02'`

---

### Amount and Quantity Columns

**Convention**: Use descriptive names with units implied by context

**Examples**:
- `total_amount` - Total dollar amount (implied USD/currency)
- `unit_price` - Price per unit
- `quantity` - Number of items
- `discount_percent` - Discount as percentage
- `tax_amount` - Tax in currency units
- `net_revenue` - Revenue after deductions

**Rationale**:
- Avoids ambiguous names like `price` (price per unit? total price?)
- Business users understand these terms
- No need for units suffix if clear from context (use comments for currency)

---

### Metadata Columns

**Convention**: Standard audit columns in every table

**Standard Columns**:
- `created_at` - Timestamp when record was created
- `updated_at` - Timestamp when record was last updated
- `created_by` - User or process that created record (optional)
- `updated_by` - User or process that last updated record (optional)

**Example**:
```sql
CREATE TABLE STAGING.stg_customers (
    customer_id NUMBER,
    customer_name VARCHAR,
    -- ... business columns ...
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

**Rationale**:
- Enables troubleshooting ("When did this record appear?")
- Supports incremental loading patterns
- Standard practice in data warehousing

---

## File Format Naming

**Convention**: `UPPERCASE_WITH_UNDERSCORES`, suffixed by format type

**Format**: `{PURPOSE}_{FORMAT_TYPE}`

**Examples**:
- `CSV_FORMAT` - Standard CSV file format
- `PARQUET_FORMAT` - Parquet file format
- `JSON_FORMAT` - JSON file format (if used)
- `CSV_GZIP_FORMAT` - Compressed CSV format (if needed)

**Rationale**:
- Uppercase distinguishes file formats from tables in SQL
- Format type suffix makes it clear what parser to expect
- Simple names are easy to reference in COPY commands

---

## External Stage Naming

**Convention**: `lowercase_with_underscores`, describing data source

**Format**: `{source}_{purpose}_stage`

**Examples**:
- `s3_raw_data_stage` - Stage pointing to raw data in S3
- `s3_transactions_stage` - Stage for transaction files
- `azure_external_stage` - Stage for Azure storage (hypothetical)

**Rationale**:
- Lowercase keeps stages visually distinct from schemas/databases
- `_stage` suffix makes purpose immediately clear
- Source prefix indicates where data comes from

---

## View Naming

**Convention**: Same as tables, optionally with `_vw` suffix

**Format**:
- `{layer_prefix}_{entity}` (same as tables)
- `{layer_prefix}_{entity}_vw` (if distinguishing views from tables is important)

**Examples**:
- `stg_active_customers` - View of active customers in staging
- `fact_sales_summary_vw` - Aggregated sales view in production
- `dim_customer_current_vw` - Current snapshot of customer dimension (SCD Type 2)

**When to Use Views**:
- Simplify complex joins for end users
- Apply row-level security (e.g., filter by region)
- Provide backward compatibility when table structures change
- Create denormalized "wide" views from normalized tables

**Rationale**:
- Views should blend seamlessly with tables in most cases
- `_vw` suffix optional - use it if your team finds it helpful
- Focus on descriptive names that explain what the view provides

---

## Procedure and Function Naming

**Convention**: `lowercase_with_underscores`, verb-first

**Format**: `{action}_{entity}` or `{action}_{entity}_{detail}`

**Examples**:
- `load_raw_transactions` - Procedure to load raw transaction data
- `refresh_staging_customers` - Procedure to refresh staging customer table
- `calculate_customer_lifetime_value` - Function to calculate CLV
- `deduplicate_records` - Procedure to remove duplicates

**Rationale**:
- Verb-first naming makes it clear this is executable code
- Describes what the procedure does, not how it does it
- Lowercase distinguishes from objects (tables, schemas)

---

## Common Abbreviations

**When abbreviations are acceptable** (widely understood):
- `id` - identifier
- `num` - number
- `qty` - quantity
- `amt` - amount
- `pct` - percent
- `avg` - average
- `max` - maximum
- `min` - minimum
- `std` - standard
- `dim` - dimension
- `fact` - fact table
- `stg` - staging
- `agg` - aggregate

**When to spell out** (less common terms):
- `customer` (not `cust`)
- `product` (not `prod` - can be confused with "production")
- `transaction` (not `txn` or `trans`)
- `description` (not `desc`)
- `category` (not `cat`)

**Rationale**: Balance brevity with clarity. Abbreviations are fine when universally understood, but spell out ambiguous terms.

---

## Examples by Layer

### Complete RAW Table Example
```sql
CREATE TABLE RAW.raw_transactions (
    InvoiceNo VARCHAR,        -- Source uses mixed case, preserve it
    InvoiceDate TIMESTAMP,
    StockCode VARCHAR,
    Description VARCHAR,
    Quantity NUMBER,
    UnitPrice DECIMAL(10,2),
    CustomerID NUMBER,
    Country VARCHAR
);
```

### Complete STAGING Table Example
```sql
CREATE TABLE STAGING.stg_transactions (
    invoice_no VARCHAR,       -- Standardized to lowercase
    invoice_date TIMESTAMP,
    stock_code VARCHAR,
    description VARCHAR,
    quantity NUMBER,
    unit_price DECIMAL(10,2),
    customer_id NUMBER,
    country VARCHAR,
    total_price DECIMAL(10,2), -- Derived column
    is_valid BOOLEAN,          -- Data quality flag
    created_at TIMESTAMP,      -- Metadata
    updated_at TIMESTAMP       -- Metadata
);
```

### Complete PRODUCTION Fact Table Example
```sql
CREATE TABLE PRODUCTION.fact_sales (
    sales_key NUMBER PRIMARY KEY,  -- Surrogate key
    date_key NUMBER,               -- Foreign key
    customer_key NUMBER,           -- Foreign key
    product_key NUMBER,            -- Foreign key
    invoice_no VARCHAR,            -- Business key
    quantity NUMBER,               -- Measure
    unit_price DECIMAL(10,2),      -- Measure
    total_amount DECIMAL(10,2),    -- Measure
    created_at TIMESTAMP
);
```

### Complete PRODUCTION Dimension Table Example
```sql
CREATE TABLE PRODUCTION.dim_customer (
    customer_key NUMBER PRIMARY KEY,  -- Surrogate key
    customer_id NUMBER,               -- Natural key from source
    customer_name VARCHAR,
    country_key NUMBER,               -- FK to dim_country
    first_order_date DATE,
    last_order_date DATE,
    total_orders NUMBER,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

---

## Anti-Patterns to Avoid

### ❌ Ambiguous Names
```sql
-- Bad
CREATE TABLE data (id NUMBER, value VARCHAR);

-- Good
CREATE TABLE stg_customer_sales (customer_id NUMBER, sales_amount DECIMAL);
```

### ❌ Inconsistent Casing
```sql
-- Bad (mixing conventions)
CREATE TABLE STG_customers (CustomerID NUMBER, customer_name VARCHAR);

-- Good
CREATE TABLE STAGING.stg_customers (customer_id NUMBER, customer_name VARCHAR);
```

### ❌ Unclear Abbreviations
```sql
-- Bad
CREATE TABLE prod.cust_prd_txn (cust_id NUMBER, prd_id NUMBER, txn_amt DECIMAL);

-- Good
CREATE TABLE PRODUCTION.fact_customer_purchases (
    customer_key NUMBER,
    product_key NUMBER,
    purchase_amount DECIMAL
);
```

### ❌ Missing Layer Indicators
```sql
-- Bad (which layer?)
CREATE TABLE transactions (...);

-- Good
CREATE TABLE RAW.raw_transactions (...);
CREATE TABLE STAGING.stg_transactions (...);
CREATE TABLE PRODUCTION.fact_sales (...);
```

---

## Summary Checklist

When creating new database objects, ask yourself:

- [ ] **Does the name indicate which layer it belongs to?** (`raw_`, `stg_`, `fact_`, `dim_`)
- [ ] **Is the name self-documenting?** (Someone unfamiliar with the project should understand it)
- [ ] **Is it consistent with existing patterns?** (Check other objects in the same layer)
- [ ] **Are abbreviations clear and standard?** (Avoid obscure abbreviations)
- [ ] **Does it follow the casing convention?** (UPPER for schemas/databases, lower for tables/columns)
- [ ] **Are keys clearly distinguished?** (`_id` for natural, `_key` for surrogate)

---

## References

This naming convention draws from:
- **Kimball Dimensional Modeling** methodology
- **Medallion Architecture** (Databricks)
- Common industry practices from Snowflake, dbt, and modern data teams

For more on the multi-layer architecture, see [data-layers.md](./data-layers.md).
