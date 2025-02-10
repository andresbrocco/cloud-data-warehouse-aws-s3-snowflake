# Snowflake Performance Optimization Guide

## Overview

Performance optimization in Snowflake is about balancing query speed, storage costs, and compute costs. This guide covers the three main optimization techniques implemented in this project: materialized views, clustering keys, and search optimization service.

## Table of Contents

1. [Optimization Strategies Overview](#optimization-strategies-overview)
2. [Materialized Views](#materialized-views)
3. [Clustering Keys](#clustering-keys)
4. [Search Optimization Service](#search-optimization-service)
5. [Query Profiling and Analysis](#query-profiling-and-analysis)
6. [Cost vs Performance Tradeoffs](#cost-vs-performance-tradeoffs)
7. [Production Best Practices](#production-best-practices)
8. [Implementation Guide](#implementation-guide)

## Optimization Strategies Overview

### When to Optimize

Not all queries need optimization. Focus optimization efforts on:

- **Frequently-run queries**: Dashboard queries, scheduled reports, API endpoints
- **Slow queries**: Anything taking > 10 seconds that could be faster
- **Expensive queries**: High bytes_scanned indicates room for improvement
- **User-facing queries**: Where sub-second response times matter

### Optimization Decision Tree

```
Is the query slow or expensive?
├── No → No optimization needed, focus elsewhere
└── Yes → Does it aggregate large datasets?
    ├── Yes → Consider Materialized View
    │   └── Is it run frequently?
    │       ├── Yes → Create MV (storage cost worth it)
    │       └── No → Keep as regular view or optimize base query
    └── No → Does it filter on specific columns?
        ├── Range filters (>, <, BETWEEN) → Add Clustering Key
        ├── Equality filters (=, IN) → Enable Search Optimization
        └── Full table scan → Reconsider query design
```

## Materialized Views

### What They Are

Materialized views (MVs) store pre-computed query results physically, unlike regular views which are virtual.

**Key characteristics:**
- Physical storage of aggregated data
- Automatic refresh when underlying tables change
- Query MVs just like regular tables (SELECT, WHERE, JOIN)
- Best for expensive aggregations over large datasets

### When to Use Materialized Views

**Good candidates:**
- Dashboard queries with complex GROUP BY aggregations
- Reports that aggregate millions of rows down to thousands
- Queries with expensive JOIN operations
- Metrics recalculated frequently (daily active users, revenue totals)

**Example: Customer Lifetime Value**
```sql
-- Without MV: Scans 500K fact rows every time
SELECT customer_id, SUM(total_amount) AS lifetime_value
FROM fact_sales
GROUP BY customer_id;

-- With MV: Scans 4K customer rows (pre-aggregated)
SELECT customer_id, lifetime_value
FROM mv_customer_summary;
```

**Performance gain:** 100x fewer rows scanned, no aggregation compute

### When NOT to Use Materialized Views

**Bad candidates:**
- Infrequently-run queries (storage cost not justified)
- Queries requiring real-time data (MV refresh has slight delay)
- Simple queries that are already fast (< 1 second)
- Ad-hoc analysis with constantly changing logic

### Refresh Behavior

**Automatic refresh:**
- Snowflake detects changes to underlying tables
- Refresh typically completes within minutes
- No manual maintenance required
- Background process consumes compute credits

**Checking refresh status:**
```sql
SHOW MATERIALIZED VIEWS IN SCHEMA production;
-- Look at 'last_altered_time' column
```

**Refresh triggers:**
- INSERT, UPDATE, DELETE on base tables
- TRUNCATE followed by new data load
- Only refreshes affected rows (incremental when possible)

### Storage and Compute Costs

**Storage costs:**
- MVs consume additional storage (charged per TB-month)
- Size typically 10-50% of base table size (due to aggregation)
- Example: 500K fact rows → 4K customer summary rows = 99% size reduction

**Compute costs:**
- Background refresh operations consume compute credits
- More frequent base table changes = more refresh compute
- Cost usually small compared to query savings (for frequently-run queries)

**Cost justification calculation:**
```
Query runs: 100 times/day
Query cost without MV: 2 seconds × $0.00056/second = $0.00112 per query
Daily query cost: $0.00112 × 100 = $0.112

MV refresh: 1 time/day, 5 seconds = $0.0028
MV storage: 10 MB × $23/TB/month ≈ $0.00023/day

Daily savings: $0.112 - $0.0028 - $0.00023 = $0.109/day = $39.79/year

Conclusion: MV saves money if query runs > 3 times/day
```

## Clustering Keys

### What They Are

Clustering organizes data physically within micro-partitions based on specified columns.

**Key characteristics:**
- Snowflake stores data in micro-partitions (50-500 MB compressed)
- Clustering sorts rows within partitions by specified columns
- Enables "partition pruning" - skipping irrelevant micro-partitions
- Automatic maintenance service keeps data clustered

### How Clustering Improves Performance

**Without clustering:**
```
Query: WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
Partitions scanned: 1000 (dates scattered across all partitions)
Data scanned: 50 GB
```

**With clustering on date:**
```
Query: WHERE date BETWEEN '2024-01-01' AND '2024-01-31'
Partitions scanned: 50 (only Jan 2024 partitions)
Data scanned: 2.5 GB (95% pruning!)
```

### Choosing Clustering Keys

**Best columns for clustering:**
1. **Date/timestamp columns**: Most queries filter by date ranges
2. **High-cardinality columns**: Many distinct values (country, product_id)
3. **Frequently filtered columns**: Used in WHERE clauses consistently

**Column selection examples:**
```sql
-- Good: Common filter pattern
ALTER TABLE fact_sales CLUSTER BY (date_key, country_key);
-- Queries like: WHERE date_key BETWEEN X AND Y AND country_key = Z

-- Avoid: Too many keys (expensive maintenance)
ALTER TABLE fact_sales CLUSTER BY (date_key, country_key, product_key, customer_key);

-- Avoid: Low-cardinality columns
ALTER TABLE fact_sales CLUSTER BY (is_active); -- Only 2 values (TRUE/FALSE)
```

**Rule of thumb:**
- Start with 1 clustering key (usually date)
- Add 1-2 more keys only if filters are very common
- More keys = higher maintenance costs

### Monitoring Clustering

**Clustering depth:**
```sql
SELECT SYSTEM$CLUSTERING_DEPTH('fact_sales', '(date_key)');
-- Returns average depth (lower = better)
-- Target: < 5 for production tables
```

**Interpreting depth:**
- Depth = 1: Perfect (each key value in 1 partition)
- Depth = 5: Acceptable (must scan 5 partitions per key value)
- Depth = 20: Poor (needs re-clustering or different key)

**Clustering information:**
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales');
-- Returns JSON with:
-- - average_overlaps: How many partitions share key values
-- - average_depth: Scan depth for typical query
-- - partition_depth_histogram: Distribution of depths
```

### Automatic Clustering Maintenance

**How it works:**
- Background service monitors clustering quality
- Automatically re-clusters data when depth degrades
- Runs during idle warehouse time when possible
- Consumes compute credits (charged to account)

**Cost control:**
```sql
-- Pause automatic clustering (if costs too high)
ALTER TABLE fact_sales SUSPEND RECLUSTER;

-- Resume automatic clustering
ALTER TABLE fact_sales RESUME RECLUSTER;

-- Remove clustering entirely
ALTER TABLE fact_sales DROP CLUSTERING KEY;
```

**Monitoring clustering costs:**
```sql
-- Query account usage (requires ACCOUNTADMIN)
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE table_name = 'FACT_SALES'
ORDER BY start_time DESC;
```

### When Clustering Isn't Worth It

**Skip clustering if:**
- Table is small (< 1 GB) - query scans are already fast
- No consistent filter patterns - queries vary wildly
- Full table scans are needed - clustering won't help
- Data is naturally ordered on load - already clustered

## Search Optimization Service

### What It Is

Search optimization creates and maintains search access paths for fast point lookups.

**Key characteristics:**
- Similar to indexes in traditional databases
- Optimizes equality predicates (WHERE col = 'value')
- Also supports substring search (WHERE col LIKE '%text%')
- Different from clustering (point lookups vs range scans)

### When to Use Search Optimization

**Good candidates:**
- High-cardinality columns: customer_id, invoice_no, stock_code, email
- Frequent equality lookups: "Find customer 12345", "Show order ABC123"
- Selective queries: Return < 1% of table rows

**Example: Customer lookup**
```sql
-- Without search optimization: Full table scan
SELECT * FROM dim_customer WHERE customer_id = '12345';
-- Scans: 4,000 rows

-- With search optimization: Direct lookup
ALTER TABLE dim_customer ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id);
SELECT * FROM dim_customer WHERE customer_id = '12345';
-- Scans: 1 row (direct access)
```

### When NOT to Use Search Optimization

**Bad candidates:**
- Low-cardinality columns: status (3 values), is_active (2 values)
- Range queries: Use clustering instead
- Rarely-queried columns: Cost not justified
- Queries returning large % of rows: Not selective enough

### Search Optimization Syntax

**Enable on specific columns:**
```sql
-- Equality search (most common)
ALTER TABLE dim_customer ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id);

-- Substring search (for text search)
ALTER TABLE dim_product ADD SEARCH OPTIMIZATION ON SUBSTRING(description);

-- Multiple columns
ALTER TABLE dim_customer ADD SEARCH OPTIMIZATION
  ON EQUALITY(customer_id, email);
```

**Disable search optimization:**
```sql
ALTER TABLE dim_customer DROP SEARCH OPTIMIZATION;
```

**Check status:**
```sql
SHOW SEARCH OPTIMIZATION IN SCHEMA production;
```

### Cost Considerations

**Storage costs:**
- Search structures consume additional storage
- Typically 5-10% of table size
- Larger for substring search (more complex structures)

**Compute costs:**
- Background maintenance as data changes
- Usually minimal for slowly-changing dimensions
- Can be significant for frequently-updated tables

**When it's worth it:**
- If query runs > 10 times/day
- If table has > 1 million rows
- If query is user-facing (API, UI) requiring sub-second response

## Query Profiling and Analysis

### Using Query History

**Finding slow queries:**
```sql
SELECT
  query_id,
  query_text,
  total_elapsed_time / 1000 AS seconds,
  bytes_scanned / (1024*1024*1024) AS gb_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'ECOMMERCE_DW'
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

**Key metrics to monitor:**
- `total_elapsed_time`: End-to-end execution (milliseconds)
- `bytes_scanned`: Data read from storage (impacts cost and speed)
- `rows_produced`: Result size
- `partitions_scanned`: Number of micro-partitions accessed

### Using Query Profile (UI)

The Query Profile in Snowflake UI provides visual analysis:

1. Click on query ID in query history
2. Go to "Profile" tab
3. Analyze execution plan

**What to look for:**
- **TableScan operators**: Check "Partitions pruned %" (higher = better)
- **Join operators**: Look for "Spillage to disk" (indicates memory pressure)
- **Aggregate operators**: High memory usage may need optimization
- **Bottlenecks**: Thick bars in flame graph show expensive operations

**Optimization actions based on profile:**
- Low pruning %: Add clustering key on filtered columns
- Expensive aggregations: Create materialized view
- Slow joins: Check join keys are indexed (foreign key columns)

### Establishing Performance Baselines

**Before optimization:**
1. Run query, note query_id
2. Record: execution_time, bytes_scanned, rows_produced
3. Check query profile for bottlenecks

**After optimization:**
1. Run same query (same WHERE clause, same result)
2. Compare metrics with baseline
3. Calculate improvement: (old_time - new_time) / old_time × 100%

**Target improvements:**
- Materialized view: 50-90% time reduction
- Clustering: 50-95% bytes_scanned reduction
- Search optimization: 90%+ time reduction for point lookups

## Cost vs Performance Tradeoffs

### The Fundamental Tradeoff

```
Spend more on:                  To save on:
- Storage (MV data)        →    - Compute (repeated queries)
- Compute (clustering)     →    - Compute (query execution)
- Storage (search paths)   →    - Compute (table scans)
```

### Decision Framework

**When to optimize:**
- Query runs frequently (daily or more)
- Query is user-facing (performance expectations)
- Query costs are measurable and significant

**When NOT to optimize:**
- Query runs rarely (weekly or less)
- Query is already fast (< 1 second)
- Table is small (< 1 GB) - optimization overhead exceeds gains

### Cost Comparison Example

**Scenario: Dashboard query runs 100 times/day, takes 5 seconds**

**Option 1: No optimization**
- Query cost: 100 queries × 5 seconds × $0.00056/sec = $0.28/day
- Annual cost: $102

**Option 2: Materialized view**
- Query cost: 100 queries × 0.1 seconds × $0.00056/sec = $0.0056/day
- MV refresh cost: 1 refresh × 10 seconds × $0.00056/sec = $0.0056/day
- Storage cost: 50 MB × $23/TB/month ≈ $0.00115/day
- Daily cost: $0.012/day
- Annual cost: $4.38

**Savings: $97.62/year (95% reduction)**

### Right-Sizing Strategy

**Start minimal:**
1. Profile queries, identify top 5 slowest
2. Apply cheapest optimization first (often clustering)
3. Measure impact, verify cost savings
4. Gradually add more optimizations based on ROI

**Monitor continuously:**
- Review query history weekly
- Check optimization costs monthly
- Remove optimizations that no longer provide value

## Production Best Practices

### 1. Optimization Workflow

**Standard process:**
```
1. Measure: Establish baseline (execution time, bytes scanned)
2. Identify: Find bottleneck (use query profile)
3. Optimize: Apply appropriate technique (MV, clustering, search)
4. Validate: Re-measure, confirm improvement
5. Monitor: Track over time, verify sustained benefit
```

### 2. Materialized View Guidelines

**Best practices:**
- Create MVs for dashboard queries (run 10+ times/day)
- Monitor refresh frequency (check last_altered_time)
- Use MVs as building blocks (MV aggregating another MV)
- Document MV purpose and refresh expectations

**Naming convention:**
```sql
mv_customer_summary    -- Clear prefix, describes content
mv_daily_sales        -- Indicates aggregation level
mv_product_revenue    -- Specifies metric focus
```

### 3. Clustering Guidelines

**Best practices:**
- Start with date columns (most common filter)
- Add 1-2 high-cardinality columns if needed
- Monitor clustering depth monthly
- Suspend clustering if costs exceed savings

**Typical clustering patterns:**
```sql
-- Time-series data
CLUSTER BY (date)

-- Multi-tenant data
CLUSTER BY (tenant_id, date)

-- Geographic data
CLUSTER BY (country, region, date)
```

### 4. Search Optimization Guidelines

**Best practices:**
- Enable on dimensions with frequent lookups
- Use for customer_id, product_id, order_id columns
- Avoid on low-cardinality columns
- Monitor usage in query profiles

**Typical search optimization:**
```sql
-- Customer dimension
ALTER TABLE dim_customer ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id, email);

-- Product dimension
ALTER TABLE dim_product ADD SEARCH OPTIMIZATION ON EQUALITY(stock_code);

-- Order fact (for order lookup APIs)
ALTER TABLE fact_orders ADD SEARCH OPTIMIZATION ON EQUALITY(order_id);
```

### 5. Monitoring and Maintenance

**Weekly checks:**
- Review top 20 slowest queries
- Check for new optimization opportunities
- Verify existing optimizations still provide value

**Monthly reviews:**
- Compare query costs (current vs previous month)
- Review clustering maintenance costs
- Evaluate MV storage costs vs query savings
- Remove optimizations with low ROI

**Quarterly audits:**
- Full performance review of all optimizations
- Update optimization strategy based on usage patterns
- Document decisions and rationale

### 6. Documentation Standards

**For each optimization, document:**
- What: Which table/view was optimized
- Why: Business justification (query frequency, performance requirement)
- How: Specific optimization applied (MV, clustering, search)
- Impact: Before/after metrics (time, cost, bytes scanned)
- Owner: Team responsible for monitoring

**Example documentation:**
```markdown
## fact_sales Clustering (2024-02-10)

**Optimization:** Added clustering key on (date_key, country_key)

**Justification:**
- 80% of queries filter by date range
- 60% of queries filter by country
- Queries scan average 50 GB before clustering

**Impact:**
- Average bytes scanned: 50 GB → 5 GB (90% reduction)
- Average query time: 12 sec → 2 sec (83% reduction)
- Clustering maintenance: $15/month

**Owner:** Data Engineering Team
**Review date:** 2024-05-10 (quarterly)
```

## Implementation Guide

### Step-by-Step Optimization Process

**1. Identify optimization candidates**
```sql
-- Find slowest queries
SELECT query_id, query_text, total_elapsed_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE database_name = 'ECOMMERCE_DW'
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

**2. Analyze with Query Profile**
- Click query_id in Snowflake UI
- Check Profile tab for bottlenecks
- Note: pruning ratio, spillage, expensive operators

**3. Choose optimization technique**
```
Expensive aggregation? → Materialized View
Range filters? → Clustering
Point lookups? → Search Optimization
```

**4. Implement optimization**
```sql
-- Example: Create MV
CREATE MATERIALIZED VIEW mv_customer_summary AS
SELECT customer_id, SUM(revenue) AS total_revenue
FROM fact_sales
GROUP BY customer_id;
```

**5. Validate improvement**
```sql
-- Run optimized query
SELECT * FROM mv_customer_summary WHERE customer_id = '12345';

-- Compare with baseline
-- Check: execution_time, bytes_scanned
```

**6. Monitor ongoing**
- Add to weekly performance review
- Track costs vs savings
- Adjust or remove if ROI declines

### Testing Optimizations

**A/B testing approach:**
```sql
-- Test 1: Original query (baseline)
SELECT customer_id, SUM(revenue)
FROM fact_sales
GROUP BY customer_id;
-- Record: query_id_A

-- Test 2: Optimized query (with MV)
SELECT customer_id, total_revenue
FROM mv_customer_summary;
-- Record: query_id_B

-- Compare results
SELECT
  'Baseline' AS version, total_elapsed_time, bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_id = 'query_id_A'
UNION ALL
SELECT
  'Optimized', total_elapsed_time, bytes_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_id = 'query_id_B';
```

## Summary

**Quick reference:**

| Optimization        | Use Case                                  | Cost                      | Speedup                    |
| ------------------- | ----------------------------------------- | ------------------------- | -------------------------- |
| Materialized View   | Frequent expensive aggregations           | Storage + Refresh compute | 50-90%                     |
| Clustering          | Range filters on large tables             | Maintenance compute       | 50-95% scan reduction      |
| Search Optimization | Point lookups on high-cardinality columns | Storage + Maintenance     | 90%+ for selective queries |

**Remember:**
- Not all queries need optimization
- Start with profiling, measure before optimizing
- Apply cheapest optimization first
- Monitor costs vs savings
- Remove optimizations with negative ROI

**Next steps:**
- Review [Snowflake Performance Documentation](https://docs.snowflake.com/en/user-guide/performance-tuning)
- Experiment with optimizations on non-production data first
