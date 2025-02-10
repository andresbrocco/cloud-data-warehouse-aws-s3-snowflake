-- ============================================================================
-- Query Performance Analysis and Monitoring
-- ============================================================================
-- Purpose: Analyze query execution patterns and identify optimization opportunities
--
-- Snowflake provides rich query history metadata for performance tuning:
-- - INFORMATION_SCHEMA.QUERY_HISTORY: Last 7 days of query execution stats
-- - ACCOUNT_USAGE.QUERY_HISTORY: Up to 365 days (1-hour latency)
--
-- Key Metrics:
-- - total_elapsed_time: End-to-end execution time (milliseconds)
-- - compilation_time: SQL parsing and optimization (usually < 1 second)
-- - execution_time: Actual query processing time
-- - queued_overload_time: Time waiting for compute resources
-- - bytes_scanned: Data read from storage (impacts cost and speed)
-- - rows_produced: Query result size
-- - partitions_scanned: Micro-partitions accessed (lower = better pruning)
--
-- Use Cases:
-- - Identify slow queries for optimization
-- - Monitor query patterns and resource usage
-- - Validate optimization impact (before/after comparison)
-- - Capacity planning (warehouse sizing decisions)
-- ============================================================================

USE DATABASE ECOMMERCE_DW;
USE SCHEMA PRODUCTION;

-- ============================================================================
-- Query 1: Recent Query History with Execution Times
-- ============================================================================
-- Purpose: Find slowest queries in last 24 hours for optimization
--
-- What to look for:
-- - High total_elapsed_time (> 10 seconds) - candidates for MVs
-- - High bytes_scanned - candidates for clustering
-- - High queued_overload_time - need larger warehouse
-- - Failed queries - investigate error_message
--
-- Note: This queries INFORMATION_SCHEMA (live view, no latency)
-- ============================================================================

SELECT
  query_id,
  query_text,
  database_name,
  schema_name,
  execution_status,
  total_elapsed_time / 1000 AS execution_time_seconds,
  bytes_scanned,
  rows_produced,
  start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'ECOMMERCE_DW'
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 20;

-- ============================================================================
-- Query 2: Queries That Scanned the Most Data
-- ============================================================================
-- Purpose: Identify data-intensive queries for clustering optimization
--
-- Why this matters:
-- - Bytes scanned correlates with query cost (Snowflake charges for compute time)
-- - Large scans indicate poor partition pruning (clustering can help)
-- - Repeated large scans are prime candidates for materialized views
--
-- Action items from results:
-- - Top query scans > 1 GB: Add clustering key to table on filtered columns
-- - Same query appears multiple times: Create materialized view
-- - Scanning entire table: Add WHERE clause or create filtered view
-- ============================================================================

SELECT
  query_id,
  LEFT(query_text, 100) AS query_snippet,
  total_elapsed_time / 1000 AS execution_time_seconds,
  bytes_scanned / (1024 * 1024 * 1024) AS gb_scanned,
  rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'ECOMMERCE_DW'
  AND bytes_scanned > 0
ORDER BY bytes_scanned DESC
LIMIT 10;

-- ============================================================================
-- Query 3: Queries by User (for audit and resource tracking)
-- ============================================================================
-- Purpose: Understand which users/workloads consume most compute
--
-- Useful for:
-- - Chargeback to business units (who uses what resources)
-- - Identifying power users who need query optimization training
-- - Detecting runaway queries or inefficient BI tool patterns
-- ============================================================================

SELECT
  user_name,
  COUNT(*) AS query_count,
  SUM(total_elapsed_time) / 1000 AS total_execution_seconds,
  AVG(total_elapsed_time) / 1000 AS avg_execution_seconds,
  SUM(bytes_scanned) / (1024 * 1024 * 1024) AS total_gb_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'ECOMMERCE_DW'
  AND execution_status = 'SUCCESS'
GROUP BY user_name
ORDER BY total_execution_seconds DESC;

-- ============================================================================
-- Query 4: Failed Queries (for debugging)
-- ============================================================================
-- Purpose: Investigate query failures to improve reliability
--
-- Common failure reasons:
-- - Syntax errors (easy to fix)
-- - Resource exceeded (warehouse too small)
-- - Permissions issues (role/grant problems)
-- - Data quality issues (division by zero, type mismatches)
-- ============================================================================

SELECT
  query_id,
  query_text,
  error_code,
  error_message,
  start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'ECOMMERCE_DW'
  AND execution_status = 'FAILED'
ORDER BY start_time DESC
LIMIT 20;

-- ============================================================================
-- Performance Monitoring Best Practices
-- ============================================================================
-- 1. Establish Baselines:
--    - Before optimization: Record execution time, bytes scanned
--    - After optimization: Re-run same query, compare metrics
--    - Target: 50%+ reduction in execution time or bytes scanned
--
-- 2. Regular Monitoring:
--    - Run these queries weekly to spot trends
--    - Alert on queries > 30 seconds (configure threshold for your needs)
--    - Create dashboard of top 10 slowest queries
--
-- 3. Optimization Workflow:
--    - Identify: Find slow/expensive queries from history
--    - Analyze: Use query profile in Snowflake UI (visual explain plan)
--    - Optimize: Apply MV, clustering, or search optimization
--    - Validate: Re-run query, check metrics in query history
--    - Document: Record what you changed and why
--
-- 4. Query Profile (Snowflake UI):
--    - Click query ID in query history
--    - View "Profile" tab for visual execution plan
--    - Look for: TableScan (pruning ratio), Join (spillage), Aggregate (memory usage)
--    - Bottlenecks show as thick bars in flame graph
--
-- 5. Cost Optimization:
--    - bytes_scanned × warehouse_cost_per_byte = query cost (approx)
--    - Reduce scans through clustering and selective queries
--    - Use result cache (free) - identical queries return cached results
--    - Right-size warehouses (don't use XL for simple queries)
-- ============================================================================

-- ============================================================================
-- Advanced: Query History from ACCOUNT_USAGE (for longer lookback)
-- ============================================================================
-- Purpose: Analyze query patterns over weeks/months for capacity planning
--
-- Differences from INFORMATION_SCHEMA:
-- - Retention: 365 days (vs 7 days in INFORMATION_SCHEMA)
-- - Latency: 45-120 minutes delay (vs real-time)
-- - Use case: Trend analysis, not real-time monitoring
--
-- Example (requires ACCOUNTADMIN role or granted privileges):
-- SELECT
--   DATE_TRUNC('DAY', start_time) AS query_date,
--   COUNT(*) AS daily_query_count,
--   SUM(total_elapsed_time) / 1000 AS daily_execution_seconds
-- FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
-- WHERE database_name = 'ECOMMERCE_DW'
--   AND start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
-- GROUP BY DATE_TRUNC('DAY', start_time)
-- ORDER BY query_date;
-- ============================================================================
