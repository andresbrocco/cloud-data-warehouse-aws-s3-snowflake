# Data Loading Guide

## Overview

This document provides comprehensive guidance for loading e-commerce transaction data from AWS S3 into Snowflake's RAW layer. The loading process is a critical component of the data warehouse pipeline, transforming raw files in cloud storage into queryable database tables.

## Architecture

### Data Flow

```
Source Files (S3)
        ↓
External Stage (Snowflake pointer to S3)
        ↓
COPY INTO Command
        ↓
RAW_LAYER Tables (Snowflake database)
```

### Components

1. **S3 Bucket**: `snowflake-ecommerce-data-andresbrocco/raw-data/`
   - Storage location for CSV and Parquet files
   - Configured with versioning and encryption
   - Access controlled via IAM roles

2. **Storage Integration**: `S3_INTEGRATION`
   - Secure connection between Snowflake and AWS
   - Uses IAM role for credential-less authentication
   - Eliminates need for hardcoded AWS keys

3. **External Stage**: `S3_ECOMMERCE_STAGE`
   - Named reference to S3 location
   - Pre-configured with storage integration
   - Default file format settings

4. **File Formats**: `CSV_FORMAT` and `PARQUET_FORMAT`
   - Reusable parsing rules for each format
   - Define delimiter, null handling, compression
   - Ensure consistent data interpretation

5. **Target Table**: `raw_transactions`
   - Destination for loaded data
   - All VARCHAR columns (permissive schema)
   - Includes audit columns for data lineage

## Loading Methods

### Method 1: CSV Loading

**File Characteristics:**
- Format: Comma-separated values
- Size: ~91 MB uncompressed
- Encoding: UTF-8
- Header: First row contains column names

**Loading Script:** `sql/raw/02_load_data_csv.sql`

**Key Features:**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE/online_retail.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;
```

**Performance Characteristics:**
- Load time: 15-30 seconds (X-Small warehouse)
- Throughput: ~40,000-70,000 rows/second
- Error handling: Continues on row-level errors
- Best for: Initial data receipt, human-readable exports

### Method 2: Parquet Loading

**File Characteristics:**
- Format: Columnar binary format
- Size: ~7.5 MB (92% smaller than CSV)
- Compression: Snappy (built-in)
- Schema: Embedded in file metadata

**Loading Script:** `sql/raw/03_load_data_parquet.sql`

**Key Features:**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE/online_retail.parquet
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
ON_ERROR = 'CONTINUE';
```

**Performance Characteristics:**
- Load time: 5-10 seconds (X-Small warehouse)
- Throughput: ~100,000-200,000 rows/second
- Error handling: Schema validation at file level
- Best for: Production data lakes, large-scale loads

## Loading Process

### Step-by-Step Workflow

#### 1. Pre-Load Validation

Verify files are accessible in S3:
```sql
LIST @S3_ECOMMERCE_STAGE PATTERN = '.*\\.csv';
```

Expected output: File listing with sizes and timestamps

#### 2. Data Preview

Query stage directly to preview structure:
```sql
SELECT $1, $2, $3, $4, $5, $6, $7, $8
FROM @S3_ECOMMERCE_STAGE/online_retail.csv
  (FILE_FORMAT => 'CSV_FORMAT')
LIMIT 10;
```

Validates:
- File format is correctly configured
- Column alignment matches expectations
- No obvious data quality issues

#### 3. Execute Load

Run COPY INTO command with explicit column mapping:
```sql
COPY INTO raw_transactions (
  invoice_no, stock_code, description, quantity,
  invoice_date, unit_price, customer_id, country,
  file_name, file_row_number
)
FROM (
  SELECT
    $1, $2, $3, $4, $5, $6, $7, $8,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER
  FROM @S3_ECOMMERCE_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*\\.csv'
ON_ERROR = 'CONTINUE';
```

#### 4. Verify Load Results

Check load history:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'raw_transactions',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
```

Key metrics:
- `row_count`: Rows successfully loaded
- `row_parsed`: Total rows processed
- `load_time`: Seconds taken to load
- `error_count`: Number of rejected rows
- `first_error_message`: Details of any errors

#### 5. Data Quality Checks

Validate loaded data:
```sql
-- Row count
SELECT COUNT(*) FROM raw_transactions;

-- Null analysis
SELECT
  SUM(CASE WHEN invoice_no IS NULL THEN 1 ELSE 0 END) AS null_invoices,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customers
FROM raw_transactions;

-- Country distribution
SELECT country, COUNT(*) AS transaction_count
FROM raw_transactions
GROUP BY country
ORDER BY transaction_count DESC
LIMIT 10;
```

## Data Lineage Tracking

### Audit Columns

Every loaded row includes metadata for troubleshooting:

**load_timestamp**
- When row was loaded into Snowflake
- Uses Snowflake server time (TIMESTAMP_NTZ)
- Automatically populated via DEFAULT constraint

**file_name**
- Source file path in S3
- Captured from `METADATA$FILENAME` pseudo-column
- Example: `raw-data/online_retail.csv`

**file_row_number**
- Row number within source file
- Captured from `METADATA$FILE_ROW_NUMBER`
- Combined with file_name, provides exact source location

### Use Cases

**Tracing Data Issues:**
```sql
-- Find which file contained a problematic order
SELECT file_name, file_row_number, invoice_no
FROM raw_transactions
WHERE invoice_no = 'C536391';
```

**Identifying Load Batches:**
```sql
-- Group data by load timestamp
SELECT
  DATE_TRUNC('hour', load_timestamp) AS load_hour,
  COUNT(*) AS rows_loaded
FROM raw_transactions
GROUP BY load_hour
ORDER BY load_hour;
```

**Reprocessing Failed Rows:**
```sql
-- Identify all rows from a specific file
SELECT *
FROM raw_transactions
WHERE file_name = 'raw-data/online_retail.csv'
  AND file_row_number BETWEEN 1000 AND 2000;
```

## Error Handling

### Common Load Errors

#### 1. File Not Found
**Symptom:** `File 'online_retail.csv' not found`

**Solutions:**
- Verify file uploaded: `LIST @S3_ECOMMERCE_STAGE;`
- Check file name spelling (case-sensitive)
- Confirm S3 bucket and prefix are correct

#### 2. Access Denied
**Symptom:** `Access denied when accessing stage`

**Solutions:**
- Verify storage integration: `DESC INTEGRATION S3_INTEGRATION;`
- Check IAM role has `s3:GetObject` and `s3:ListBucket` permissions
- Confirm IAM trust relationship includes Snowflake user ARN and external ID

#### 3. Column Count Mismatch
**Symptom:** `Number of columns in file (8) does not match number of columns in table (11)`

**Solutions:**
- Verify SKIP_HEADER = 1 in file format
- Confirm explicit column mapping in COPY INTO
- Check CSV doesn't have extra trailing delimiters

#### 4. Data Type Conversion Errors
**Symptom:** `Numeric value 'ABC' is not recognized`

**Solutions:**
- Use VARCHAR for all RAW layer columns (already done)
- Implement type validation in STAGING layer
- Use `TRY_CAST` for safe type conversion
- Set ON_ERROR = 'CONTINUE' to load valid rows

### Error Recovery Strategies

**Strategy 1: Continue on Error**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE
ON_ERROR = 'CONTINUE'  -- Load valid rows, skip invalid ones
RETURN_FAILED_ONLY = TRUE;
```

**Strategy 2: Validation Mode**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE
VALIDATION_MODE = 'RETURN_ERRORS';  -- Preview errors without loading
```

**Strategy 3: Skip File on High Error Rate**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE
ON_ERROR = 'SKIP_FILE_5%';  -- Skip file if >5% rows have errors
```

## Performance Optimization

### Warehouse Sizing

| Warehouse Size | Credits/Hour | Load Time (CSV) | Load Time (Parquet) | Recommended For           |
| -------------- | ------------ | --------------- | ------------------- | ------------------------- |
| X-Small        | 1            | 25-30s          | 8-10s               | Development, small files  |
| Small          | 2            | 13-15s          | 4-5s                | Testing, medium files     |
| Medium         | 4            | 7-8s            | 2-3s                | Production, large files   |
| Large          | 8            | 4-5s            | 1-2s                | Bulk loads, time-critical |

**Recommendation:** Start with X-Small for development. Scale up only if load times exceed SLA requirements.

### File Size Best Practices

**Optimal File Sizes:**
- Single file: 100-500 MB compressed
- Multiple files: Total 1-5 GB per load batch
- Max single file: Avoid files > 1 GB

**Rationale:**
- Too small (< 10 MB): Overhead from file discovery and metadata operations
- Too large (> 1 GB): Limited parallelization, slow single-threaded processing
- Multiple medium files: Snowflake loads in parallel for better throughput

### Parallel Loading

Load multiple files simultaneously:
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*transactions_2024.*\\.csv';  -- Matches multiple files
```

Snowflake automatically:
- Detects all matching files
- Distributes work across compute nodes
- Processes files in parallel
- Aggregates results

### Incremental Loading

For daily updates, use pattern matching with timestamps:
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*transactions_2024_02_02.*\\.csv'  -- Today's file only
ON_ERROR = 'CONTINUE';
```

Track loaded files to avoid duplicates:
```sql
-- Create load tracking table
CREATE TABLE load_history (
  file_name VARCHAR,
  load_date DATE,
  row_count NUMBER
);

-- Check before loading
SELECT file_name
FROM load_history
WHERE file_name = 'online_retail.csv';
```

## Format Comparison

### CSV vs Parquet Performance

| Metric                  | CSV      | Parquet   | Winner  | Improvement |
| ----------------------- | -------- | --------- | ------- | ----------- |
| File Size               | 91 MB    | 7.5 MB    | Parquet | 92% smaller |
| Load Time (X-Small)     | 25s      | 8s        | Parquet | 3.1x faster |
| Throughput (rows/sec)   | 42,000   | 133,000   | Parquet | 3.2x faster |
| S3 Storage Cost/month   | $0.00209 | $0.00017  | Parquet | 92% cheaper |
| Data Transfer Cost/load | $0.00819 | $0.00068  | Parquet | 92% cheaper |
| Human Readable          | Yes      | No        | CSV     | N/A         |
| Schema Evolution        | Manual   | Automatic | Parquet | N/A         |

**Detailed benchmark:** See `sql/raw/04_benchmark_csv_vs_parquet.sql` for full comparison.

### When to Use Each Format

**Use CSV when:**
- ✅ Receiving initial data from source systems (compatibility)
- ✅ Exporting for human review or business users
- ✅ Working with small files (< 10 MB)
- ✅ Debugging data quality issues (readable format)
- ✅ Source system only supports CSV exports

**Use Parquet when:**
- ✅ Storing data in S3 data lake (production workloads)
- ✅ Loading large datasets (> 100 MB)
- ✅ Daily/frequent data loads (cost optimization)
- ✅ Schema evolution is expected
- ✅ Integration with big data tools (Spark, Hive, Presto)

**Recommended Workflow:**
1. Receive CSV from source systems
2. Convert to Parquet using Python/Pandas or AWS Glue
3. Upload Parquet to S3
4. Load into Snowflake from Parquet

Conversion example (Python/Pandas):
```python
import pandas as pd

# Read CSV
df = pd.read_csv('online_retail.csv')

# Write Parquet
df.to_parquet(
    'online_retail.parquet',
    compression='snappy',
    index=False
)
```

## Troubleshooting

### Diagnostic Queries

**Check Stage Configuration:**
```sql
DESC STAGE S3_ECOMMERCE_STAGE;
```

**List Files in Stage:**
```sql
LIST @S3_ECOMMERCE_STAGE;
```

**View Recent Load History:**
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'raw_transactions',
  START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
```

**Find Failed Loads:**
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'raw_transactions',
  START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
WHERE status = 'LOAD_FAILED'
ORDER BY last_load_time DESC;
```

**Preview Errors Without Loading:**
```sql
COPY INTO raw_transactions
FROM @S3_ECOMMERCE_STAGE/online_retail.csv
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
VALIDATION_MODE = 'RETURN_ERRORS';
```

### Performance Issues

**Slow Loads (> 60 seconds):**
1. Check warehouse size: `SHOW WAREHOUSES;`
2. Verify file is compressed in S3
3. Consider splitting large files into smaller chunks
4. Use Parquet instead of CSV
5. Scale up warehouse temporarily

**High Error Rates (> 1%):**
1. Review error messages: Check `first_error_message` in COPY_HISTORY
2. Validate file format settings: `DESC FILE FORMAT CSV_FORMAT;`
3. Preview data: `SELECT * FROM @stage/file LIMIT 10;`
4. Check for encoding issues (UTF-8 vs. Latin-1)
5. Use VALIDATION_MODE to identify problematic rows

## Best Practices

### Security
- ✅ Use storage integration (not hardcoded credentials)
- ✅ Implement IAM least-privilege access
- ✅ Enable S3 bucket encryption
- ✅ Audit data access with Snowflake query history
- ✅ Rotate IAM role credentials regularly

### Reliability
- ✅ Always preview data before full load
- ✅ Use ON_ERROR = 'CONTINUE' for fault tolerance
- ✅ Capture file_name and file_row_number for lineage
- ✅ Verify row counts after loading
- ✅ Implement retry logic for transient errors

### Performance
- ✅ Prefer Parquet over CSV for production
- ✅ Right-size warehouse (don't over-provision)
- ✅ Load multiple files in parallel when possible
- ✅ Use pattern matching for incremental loads
- ✅ Monitor load times and optimize as data grows

### Maintainability
- ✅ Use named file formats (not inline definitions)
- ✅ Document expected file sizes and row counts
- ✅ Create reusable SQL scripts
- ✅ Include validation and verification steps
- ✅ Keep load history for auditing

## Related Documentation

- **Setup Scripts:**
  - [Storage Integration](../sql/setup/01_storage_integration.sql)
  - [File Formats](../sql/setup/03_create_file_formats.sql)
  - [External Stage](../sql/setup/04_create_external_stage.sql)

- **Loading Scripts:**
  - [CSV Loading](../sql/raw/02_load_data_csv.sql)
  - [Parquet Loading](../sql/raw/03_load_data_parquet.sql)
  - [Performance Benchmark](../sql/raw/04_benchmark_csv_vs_parquet.sql)

- **Architecture:**
  - [Data Layers](architecture/data-layers.md)
  - [Naming Conventions](architecture/naming-conventions.md)

- **Configuration:**
  - [AWS Setup](aws-setup.md)
  - [Snowflake Setup](snowflake-setup.md)

## Next Steps

After successfully loading data into the RAW layer:

1. ✅ **Verify Data Quality** - Run data quality checks
2. ⬜ **Transform to STAGING** - Apply cleaning and validation (sql/staging/)
3. ⬜ **Build Dimensional Model** - Create fact and dimension tables (sql/production/)
4. ⬜ **Implement Analytics** - Build analytical queries (sql/analytics/)
5. ⬜ **Schedule Loads** - Automate with orchestration tool (Airflow, dbt, etc.)
