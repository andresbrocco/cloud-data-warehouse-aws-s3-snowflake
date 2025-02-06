# Snowflake Schema Diagram

## Overview

This document provides visual representations of the snowflake schema dimensional model implemented in the PRODUCTION layer. The diagrams illustrate the relationships between fact and dimension tables, highlighting the normalized structure that characterizes a snowflake schema.

---

## Full Snowflake Schema (With Future Fact Table)

```
                             SNOWFLAKE SCHEMA DIMENSIONAL MODEL
                          ===============================================

                         ┌────────────────────────────────┐
                         │        dim_country             │
                         │     (Type 1 SCD)               │
                         ├────────────────────────────────┤
                         │ ◆ country_key (PK)             │
                         │   country_name                 │
                         │   country_code                 │
                         │   region                       │
                         └──────────────┬─────────────────┘
                                        │
                                        │ country_key (FK)
                                        │
                         ┌──────────────▼─────────────────┐
                         │       dim_customer             │
                         │     (Type 2 SCD)               │
                         ├────────────────────────────────┤
                         │ ◆ customer_key (PK)            │
                         │   customer_id                  │
                         │ ○ country_key (FK)             │
                         │   first_order_date             │
                         │   last_order_date              │
                         │   total_lifetime_orders        │
                         │   _effective_from              │
                         │   _effective_to                │
                         │   _is_current                  │
                         └──────────────┬─────────────────┘
                                        │
                                        │ customer_key (FK)
                                        │
┌────────────────────────┐             │             ┌────────────────────────┐
│     dim_category       │             │             │       dim_date         │
│     (Type 1 SCD)       │             │             │     (Static)           │
├────────────────────────┤             │             ├────────────────────────┤
│ ◆ category_key (PK)    │             │             │ ◆ date_key (PK)        │
│   category_name        │             │             │   date                 │
└──────────┬─────────────┘             │             │   year                 │
           │                           │             │   quarter              │
           │                           │             │   month                │
           │                  ┌────────▼─────────────┤   month_name           │
           │                  │    fact_sales        ├───day                  │
           │                  │    (FUTURE)          │   day_of_week          │
           │                  ├──────────────────────┤   day_name             │
           │                  │ ◆ sales_key (PK)     │   is_weekend           │
           │                  │   invoice_no         │└────────────────────────┘
           │                  │   invoice_line_no    │
           │                  │ ○ customer_key (FK)  │
           │                  │ ○ product_key (FK)   │
           │ category_key(FK) │ ○ invoice_date_key(FK)
           │                  │   quantity           │
           │                  │   unit_price         │
           │                  │   total_amount       │
           │                  │   _loaded_at         │
           │                  └────────┬─────────────┘
           │                           │
           │                           │ product_key (FK)
           │                           │
           │                  ┌────────▼─────────────┐
           │                  │     dim_product      │
           │                  │     (Type 2 SCD)     │
           │                  ├──────────────────────┤
           │                  │ ◆ product_key (PK)   │
           └──────────────────┤   stock_code         │
                              │   description        │
                              │ ○ category_key (FK)  │
                              │   unit_price         │
                              │   first_sold_date    │
                              │   _effective_from    │
                              │   _effective_to      │
                              │   _is_current        │
                              └──────────────────────┘


Legend:
  ◆ = Primary Key
  ○ = Foreign Key
  ─── = One-to-Many Relationship
  ▼ = Relationship Direction (points to child)
```

---

## Current Implementation Status (Dimensions Only)

```
                         CURRENT DIMENSIONAL MODEL (STEP 9)
                        ====================================

                         ┌────────────────────────┐
                         │      dim_country       │
                         │    ~40 countries       │
                         ├────────────────────────┤
                         │ ◆ country_key          │
                         │   country_name         │
                         │   country_code         │
                         │   region               │
                         └───────────┬────────────┘
                                     │
                                     │ Normalized hierarchy
                                     │
                         ┌───────────▼────────────┐
                         │    dim_customer        │
                         │   ~4,000 customers     │
                         ├────────────────────────┤
                         │ ◆ customer_key         │
                         │   customer_id          │
                         │ ○ country_key (FK)     │
                         │   first_order_date     │
                         │   last_order_date      │
                         │   total_lifetime_orders│
                         │   [SCD Type 2 columns] │
                         └────────────────────────┘


┌────────────────────────┐              ┌────────────────────────┐
│    dim_category        │              │      dim_date          │
│    8 categories        │              │   1,461 days           │
├────────────────────────┤              ├────────────────────────┤
│ ◆ category_key         │              │ ◆ date_key (YYYYMMDD)  │
│   category_name        │              │   date                 │
└───────────┬────────────┘              │   year, quarter, month │
            │                           │   month_name, day_name │
            │ Normalized hierarchy      │   is_weekend           │
            │                           └────────────────────────┘
            │
┌───────────▼────────────┐
│     dim_product        │
│  ~3,000-4,000 products │
├────────────────────────┤
│ ◆ product_key          │
│   stock_code           │
│   description          │
│ ○ category_key (FK)    │
│   unit_price           │
│   first_sold_date      │
│   [SCD Type 2 columns] │
└────────────────────────┘
```

---

## Normalized Hierarchies (Snowflake Pattern)

### Customer → Country Hierarchy

```
┌─────────────────────────────┐
│      dim_customer           │
│                             │
│  customer_key = 1001        │
│  customer_id = 12345        │
│  country_key = 25  ─────────┼───┐
│  first_order_date           │   │
│  last_order_date            │   │
│  total_lifetime_orders      │   │
└─────────────────────────────┘   │
                                  │
                                  │ Foreign Key Lookup
                                  │
                       ┌──────────▼────────────┐
                       │   dim_country         │
                       │                       │
                       │  country_key = 25     │
                       │  country_name = "UK"  │
                       │  country_code = "GB"  │
                       │  region = "Europe"    │
                       └───────────────────────┘
```

**Benefits:**
- Country name stored once, not repeated for every customer
- Regional assignments managed centrally
- Easy to update all customers' regional grouping

---

### Product → Category Hierarchy

```
┌─────────────────────────────┐
│      dim_product            │
│                             │
│  product_key = 5001         │
│  stock_code = "22423"       │
│  description = "Lamp"       │
│  category_key = 2  ─────────┼───┐
│  unit_price = 15.99         │   │
│  first_sold_date            │   │
└─────────────────────────────┘   │
                                  │
                                  │ Foreign Key Lookup
                                  │
                       ┌──────────▼─────────────┐
                       │   dim_category         │
                       │                        │
                       │  category_key = 2      │
                       │  category_name =       │
                       │   "Home & Garden"      │
                       └────────────────────────┘
```

**Benefits:**
- Category name stored once, not repeated for every product
- Product categorization managed centrally
- Easy to recategorize products or split categories

---

## Relationship Cardinalities

```
dim_country (1) ──< (many) dim_customer
  One country has many customers
  Each customer belongs to one country

dim_category (1) ──< (many) dim_product
  One category contains many products
  Each product belongs to one category

dim_date (1) ──< (many) fact_sales (FUTURE)
  One date has many sales transactions
  Each sale occurs on one date

dim_customer (1) ──< (many) fact_sales (FUTURE)
  One customer makes many purchases
  Each sale is made by one customer

dim_product (1) ──< (many) fact_sales (FUTURE)
  One product appears in many sales
  Each sale line item contains one product
```

---

## SCD Type 2 Version Tracking

### Customer SCD Example: Country Change

```
Initial Version (customer moves from USA to UK):

┌───────────────────────────────────────────────────────────────┐
│                    dim_customer                               │
├───────────────────────────────────────────────────────────────┤
│ customer_key │ customer_id │ country_key │ _effective_from    │ _effective_to │ _is_current │
│     1001     │    12345    │      1      │ 2010-01-15        │ 2011-06-20    │   FALSE     │  ◄── Old version (USA)
│     1002     │    12345    │     25      │ 2011-06-20        │     NULL      │   TRUE      │  ◄── Current version (UK)
└───────────────────────────────────────────────────────────────┘
                                                 │
                                                 ├─── Same customer_id (business key)
                                                 │
                                                 └─── Two versions (surrogate keys differ)
```

**Query patterns:**

```sql
-- Get current version only
SELECT * FROM dim_customer WHERE _is_current = TRUE AND customer_id = 12345;
-- Returns: customer_key = 1002, country_key = 25 (UK)

-- Get version as of 2010-12-01 (before move)
SELECT * FROM dim_customer
WHERE customer_id = 12345
  AND '2010-12-01' BETWEEN _effective_from AND COALESCE(_effective_to, '9999-12-31');
-- Returns: customer_key = 1001, country_key = 1 (USA)

-- Get full change history
SELECT * FROM dim_customer WHERE customer_id = 12345 ORDER BY _effective_from;
-- Returns both versions showing the move
```

---

### Product SCD Example: Price Change

```
Price changes from £10.00 to £12.50:

┌────────────────────────────────────────────────────────────────┐
│                    dim_product                                 │
├────────────────────────────────────────────────────────────────┤
│ product_key │ stock_code │ unit_price │ _effective_from │ _effective_to │ _is_current │
│     5001    │   22423    │   10.00    │ 2009-12-01     │ 2010-08-15    │   FALSE     │  ◄── Old price
│     5002    │   22423    │   12.50    │ 2010-08-15     │     NULL      │   TRUE      │  ◄── Current price
└────────────────────────────────────────────────────────────────┘
                                             │
                                             ├─── Same stock_code (business key)
                                             │
                                             └─── Two versions track price change
```

---

## Comparison: Star Schema vs. Snowflake Schema

### Star Schema (Denormalized)

```
                    ┌────────────────────┐
                    │   dim_customer     │
                    ├────────────────────┤
                    │ customer_key (PK)  │
                    │ customer_id        │
                    │ country_name       │  ◄── Repeated for each customer
                    │ country_code       │  ◄── Repeated for each customer
                    │ region             │  ◄── Repeated for each customer
                    │ first_order_date   │
                    │ ...                │
                    └──────────┬─────────┘
                               │
                    ┌──────────▼─────────┐
                    │    fact_sales      │
                    └────────────────────┘

Pros: Simpler queries (fewer joins)
Cons: More storage (repeated country attributes)
```

---

### Snowflake Schema (Normalized - Our Implementation)

```
        ┌────────────────────┐
        │   dim_country      │
        ├────────────────────┤
        │ country_key (PK)   │
        │ country_name       │  ◄── Stored once
        │ country_code       │  ◄── Stored once
        │ region             │  ◄── Stored once
        └──────────┬─────────┘
                   │
        ┌──────────▼─────────┐
        │   dim_customer     │
        ├────────────────────┤
        │ customer_key (PK)  │
        │ customer_id        │
        │ country_key (FK)   │  ◄── Reference instead of duplication
        │ first_order_date   │
        │ ...                │
        └──────────┬─────────┘
                   │
        ┌──────────▼─────────┐
        │    fact_sales      │
        └────────────────────┘

Pros: Less storage (no repeated attributes), easier updates
Cons: Slightly more complex queries (additional join)
```

---

## Entity-Relationship Notation

```
Entity Symbols:
  ┌─────────┐
  │  Table  │  = Table/Entity
  └─────────┘

  ◆ = Primary Key
  ○ = Foreign Key

Relationship Symbols:
  ──── = Relationship line
  ─────< = One-to-Many (crow's foot)
  ─────▼ = Directional (parent to child)

Cardinality:
  (1) ──< (many)  = One-to-Many
  (1) ─── (1)     = One-to-One
  (0..1) ──< (*)  = Optional One-to-Many
```

---

## Query Path Examples

### Regional Sales Analysis Query Path

```
Query: Total revenue by region

┌──────────────┐
│  fact_sales  │
│  (Start)     │
└──────┬───────┘
       │ customer_key (FK)
       ▼
┌──────────────┐
│ dim_customer │
└──────┬───────┘
       │ country_key (FK)
       ▼
┌──────────────┐
│ dim_country  │  ◄── region (Target attribute)
│  (End)       │
└──────────────┘

Query traverses 2 foreign keys to reach region.
```

---

### Category Performance Query Path

```
Query: Total units sold by category

┌──────────────┐
│  fact_sales  │
│  (Start)     │
└──────┬───────┘
       │ product_key (FK)
       ▼
┌──────────────┐
│ dim_product  │
└──────┬───────┘
       │ category_key (FK)
       ▼
┌──────────────┐
│ dim_category │  ◄── category_name (Target attribute)
│  (End)       │
└──────────────┘

Query traverses 2 foreign keys to reach category.
```

---

## Future Fact Table Structure

The fact_sales table (to be implemented in Step 10) will complete the dimensional model:

```
┌──────────────────────────────────┐
│          fact_sales              │
├──────────────────────────────────┤
│ ◆ sales_key (PK)                 │  ◄── Surrogate key
│   invoice_no                     │  ◄── Business key (order ID)
│   invoice_line_no                │  ◄── Line item number
│ ○ customer_key (FK)              │  ◄── → dim_customer
│ ○ product_key (FK)               │  ◄── → dim_product
│ ○ invoice_date_key (FK)          │  ◄── → dim_date
│   quantity                       │  ◄── Additive measure
│   unit_price                     │  ◄── Semi-additive measure
│   total_amount                   │  ◄── Additive measure
│   _loaded_at                     │  ◄── Metadata
└──────────────────────────────────┘

Measure Types:
  - Additive: Can be summed across all dimensions (quantity, total_amount)
  - Semi-additive: Cannot be summed across time (unit_price, balances)
  - Non-additive: Cannot be summed (ratios, percentages)
```

---

## Conclusion

This snowflake schema implements a normalized dimensional model that balances:
- **Storage efficiency** through normalization (reduced redundancy)
- **Query flexibility** via hierarchical relationships
- **Historical tracking** using SCD Type 2 on key dimensions
- **Analytical power** through proper dimensional modeling principles

The normalized hierarchies (customer→country, product→category) reduce data redundancy while maintaining query simplicity through Snowflake's efficient join processing.

Next step: Implement fact_sales table to enable end-to-end analytical queries across all dimensions.
