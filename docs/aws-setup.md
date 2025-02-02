# AWS S3 Setup Documentation

## Overview

This document details the AWS S3 configuration for the Cloud Data Warehouse project. The S3 bucket serves as the landing zone for raw e-commerce data before ingestion into Snowflake.

## S3 Bucket Configuration

### Bucket Details

- **Bucket Name:** `snowflake-ecommerce-data-andresbrocco`
- **Region:** `sa-east-1`
- **ARN:** `arn:aws:s3:::snowflake-ecommerce-data-andresbrocco`
- **Created:** February 2, 2025

### Security Settings

✅ **Block Public Access**: All public access blocked (all 4 settings enabled)
- Block public access to buckets and objects granted through new access control lists (ACLs)
- Block public access to buckets and objects granted through any access control lists (ACLs)
- Block public access to buckets and objects granted through new public bucket or access point policies
- Block public and cross-account access to buckets and objects through any public bucket or access point policies

✅ **Versioning**: Enabled
- Protects against accidental overwrites and deletions
- Maintains history of object modifications
- Essential for data lineage and auditing

✅ **Encryption**: Server-side encryption with Amazon S3 managed keys (SSE-S3)
- Enabled by default for all objects
- Automatic encryption at rest
- No additional cost

### Folder Structure

```
[bucket-name]/
├── raw-data/          # Landing zone for CSV files from Kaggle
│                      # Will be populated in Step 4
└── archive/           # Optional backup location
                       # For historical data or replaced files
```

### Tags Applied

| Key         | Value         |
| ----------- | ------------- |
| Project     | DataWarehouse |
| Environment | Development   |

## Integration with Snowflake

This bucket will be integrated with Snowflake using:
1. **Storage Integration** Uses IAM role for secure access
2. **External Stage** Points to `s3://snowflake-ecommerce-data-andresbrocco/raw-data/`
3. **COPY INTO** commands - Loads data from S3 into Snowflake tables

## Access Control

**Current:** No public access, no IAM policies yet

**Next Steps:**
- Step 5 will create IAM role with S3 read permissions
- IAM role will be trusted by Snowflake via external ID
- Principle of least privilege: only read access to `raw-data/` prefix

## Cost Considerations

- **Storage:** ~$0.023 per GB/month (us-east-1 Standard storage)
- **Dataset size:** < 100 MB (minimal cost, < $0.01/month)
- **Requests:** Negligible for batch loading pattern
- **Data transfer:** Same-region transfer to Snowflake has no cost

## References

- High-level setup documented in [README.md](../README.md#6-set-up-aws-s3)
- Environment variables: `.env` (not committed to git)
- AWS Console: https://s3.console.aws.amazon.com/s3/buckets/snowflake-ecommerce-data-andresbrocco

## Notes

- Bucket name is globally unique across all AWS accounts
- Region selected to match Snowflake account region for optimal performance
- Data upload will happen in Step 4 after dataset preparation
- IAM configuration will be completed in Step 5
