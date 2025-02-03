# Snowflake Account Setup Documentation

## Overview

This document details the Snowflake configuration for the Cloud Data Warehouse project. Snowflake serves as the analytical data warehouse platform hosting all data layers (Raw, Staging, Production) and analytics workloads.

## Account Details

### Connection Information

- **Account Identifier:** `OWKCBOM-SW05387`
- **Account Locator:** `MV13917`
- **Cloud Provider:** AWS
- **Region:** `AWS_SA_EAST_1`
- **Account URL:** `OWKCBOM-SW05387.snowflakecomputing.com`
- **Created:** February 3, 2025

### Account Tier

- **Edition:** `Standard`
- **Trial Status:** `Free trial with $400 credit`

## Virtual Warehouse Configuration

### Primary Warehouse: COMPUTE_WH

- **Name:** `COMPUTE_WH` (default warehouse)
- **Size:** X-Small (1 credit/hour when running)
- **Scaling Policy:** Standard
- **Min Clusters:** 1
- **Max Clusters:** 1
- **Auto Suspend:** 10 minutes of inactivity
- **Auto Resume:** Enabled
- **Initially Suspended:** Yes

### Cost Optimization Settings

The warehouse is configured for cost efficiency:
- **Auto-suspend after 10 minutes** - Warehouse automatically suspends when idle, preventing unnecessary charges
- **X-Small size** - Smallest warehouse size, sufficient for this project's workload (~$2/hour when running)
- **Single cluster** - No auto-scaling needed for portfolio workload

**Estimated monthly cost:** < $5 (assuming 2-3 hours of active query time)

## Connection Test Results

### Test Query Executed

```sql
SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE();
```

### Results

- **Snowflake Version:** `9.36.1`
- **Current User:** `andresbrocco`
- **Current Role:** `ACCOUNTADMIN`
- **Test Status:** ✅ Successful

## User and Role Configuration

### Primary User

- **Username:** `andresbrocco`
- **Role:** `ACCOUNTADMIN`
- **Authentication:** Username/Password (stored in `.env`)

### Role Permissions

For this portfolio project, we're using **ACCOUNTADMIN** role which provides:
- Full administrative privileges
- Ability to create databases, schemas, warehouses
- Ability to manage users and roles
- Ability to configure storage integrations

**Production Note:** In a production environment, you would use more granular roles like:
- `SYSADMIN` - For creating databases and warehouses
- `SECURITYADMIN` - For managing users and roles
- `DATAENGINEER` - Custom role with specific data pipeline permissions

## Integration with AWS S3

This Snowflake account will integrate with the AWS S3 bucket configured in Step 2:

- **S3 Bucket:** `snowflake-ecommerce-data-andresbrocco`
- **S3 Region:** `sa-east-1`
- **Integration Method:** Storage Integration with IAM role (configured in Step 5)
- **Data Flow:** S3 → Snowflake External Stage → Snowflake Tables

**Region Alignment:** `same region`

## Snowflake Web Interface (Snowsight)

The Snowflake web interface provides:
- **Worksheets:** For writing and executing SQL queries
- **Databases:** For browsing database objects
- **Data:** For exploring table data
- **Dashboards:** For visualizations (optional for this project)
- **Monitoring:** For query history and warehouse usage

## Environment Configuration

Connection details are stored in `.env` file (not committed to git):

```bash
SNOWFLAKE_ACCOUNT=[account_identifier]
SNOWFLAKE_USER=[username]
SNOWFLAKE_PASSWORD=[password]
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

## Next Steps

With Snowflake account configured, the next steps will be:

1. **Step 4:** Download and prepare e-commerce dataset
2. **Step 5:** Configure AWS IAM and Snowflake Storage Integration (secure S3 access)
3. **Step 6:** Create database and multi-layer schema architecture
4. **Step 7:** Begin data ingestion from S3

## Monitoring and Cost Management

### Tracking Usage

- Navigate to "Admin" → "Usage" to view:
  - Warehouse credit consumption
  - Storage usage
  - Data transfer costs

### Cost Alerts (Optional)

- Set up usage alerts in Snowflake to monitor credit consumption
- Recommended: Alert at 50% and 80% of trial credit usage

### Shutting Down

When not actively working on the project:
- Warehouse will auto-suspend after 10 minutes
- No action needed - automatic cost savings
- Can manually suspend via Admin → Warehouses if desired

## References

- High-level setup documented in [README.md](../README.md)
- Environment variables: `.env` (not committed to git)
- Snowflake Documentation: https://docs.snowflake.com
- Snowflake Console: `https://OWKCBOM-SW05387.snowflakecomputing.com`

## Notes

- Free trial includes $400 credit, sufficient for multiple portfolio projects
- X-Small warehouse costs ~$2/hour when actively running
- Auto-suspend feature ensures you only pay for compute when queries are running
- Storage costs are minimal (~$23/TB/month, our dataset is < 1 GB)
- Connection method uses username/password; production would use key-pair or SSO