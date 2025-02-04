# AWS IAM and Snowflake Storage Integration Setup Guide

Complete guide to establish secure, passwordless access from Snowflake to AWS S3.

## Overview

This setup creates a trust relationship between Snowflake and AWS using IAM roles and storage integrations. No AWS access keys or secrets are stored in Snowflake—authentication uses temporary STS tokens.

**Security Benefits:**
- No long-lived credentials stored in Snowflake
- Least-privilege access (read-only to specific S3 bucket)
- AWS STS generates temporary credentials automatically
- External ID prevents confused deputy attacks

## Prerequisites

Before starting, ensure you have:
- AWS account with IAM permissions
- Snowflake account with ACCOUNTADMIN role
- S3 bucket name from Step 2
- AWS account ID (12-digit number)

## Part A: AWS IAM Setup

### Step 1: Create IAM Policy for S3 Access

1. **Navigate to IAM Policies**
   - Go to https://console.aws.amazon.com/iam/
   - Click "Policies" in the left sidebar
   - Click "Create policy"

2. **Create Policy with JSON**
   - Select the "JSON" tab
   - Copy the policy from `docs/aws-iam-policy.json`
   - Replace `YOUR-BUCKET-NAME` with your actual S3 bucket name
   - The policy grants these permissions:
     - `s3:GetObject` - Read objects
     - `s3:GetObjectVersion` - Read versioned objects
     - `s3:ListBucket` - List bucket contents
     - `s3:GetBucketLocation` - Get bucket region

3. **Review and Create**
   - Click "Next: Tags" (optional, can skip)
   - Click "Next: Review"
   - Policy name: `snowflake-s3-access-policy`
   - Description: "Read-only access to S3 bucket for Snowflake data warehouse"
   - Click "Create policy"

### Step 2: Create IAM Role with Placeholder Trust Policy

1. **Navigate to IAM Roles**
   - Click "Roles" in the left sidebar
   - Click "Create role"

2. **Select Custom Trust Policy**
   - For "Trusted entity type", select **"Custom trust policy"**
   - Paste this placeholder policy:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Service": "s3.amazonaws.com"
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```

   - Click "Next"
   - **Note:** This is temporary. We'll update it after creating the Snowflake storage integration.

3. **Attach Permissions Policy**
   - Search for `snowflake-s3-access-policy`
   - Check the box next to it
   - Click "Next"

4. **Name and Create Role**
   - Role name: `snowflake-s3-integration-role`
   - Description: "IAM role for Snowflake to access S3 bucket via storage integration"
   - Click "Create role"

5. **Copy Role ARN**
   - After creation, find your role in the list
   - Click on `snowflake-s3-integration-role`
   - Copy the **ARN** (looks like `arn:aws:iam::123456789012:role/snowflake-s3-integration-role`)
   - Save this ARN—you'll need it for Snowflake

## Part B: Snowflake Storage Integration

### Step 3: Create Storage Integration in Snowflake

1. **Open Snowflake Worksheets**
   - Go to https://app.snowflake.com/
   - Click "Worksheets" in the left sidebar
   - Create a new worksheet

2. **Ensure You're Using ACCOUNTADMIN Role**
   ```sql
   USE ROLE ACCOUNTADMIN;
   ```

3. **Run Storage Integration Command**
   - Open `sql/setup/01_storage_integration.sql`
   - Replace placeholders:
     - `YOUR-ACCOUNT-ID` → Your 12-digit AWS account ID
     - `YOUR-BUCKET-NAME` → Your S3 bucket name
   - Execute the CREATE STORAGE INTEGRATION command

   Expected output: "Storage integration S3_INTEGRATION successfully created."

### Step 4: Retrieve Snowflake IAM User Details

1. **Describe the Storage Integration**
   ```sql
   DESC STORAGE INTEGRATION s3_integration;
   ```

2. **Find and Copy These Two Values:**

   | Property                    | Value to Copy                                      |
   |-----------------------------|----------------------------------------------------|
   | `STORAGE_AWS_IAM_USER_ARN`  | `arn:aws:iam::123456789012:user/abc12345-s`        |
   | `STORAGE_AWS_EXTERNAL_ID`   | `ABC12345_SFCRole=1_aBcDeFgHiJkLmNoPqRsTuVwXyZ=`   |

   **Important:** Copy both values exactly—you'll need them for the AWS trust policy.

## Part C: Update AWS IAM Trust Policy

### Step 5: Update IAM Role Trust Relationship

1. **Navigate Back to IAM Role**
   - Go to AWS Console → IAM → Roles
   - Find and click `snowflake-s3-integration-role`

2. **Edit Trust Policy**
   - Click the "Trust relationships" tab
   - Click "Edit trust policy"

3. **Replace Trust Policy**
   - Delete the placeholder policy
   - Paste this new policy:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "PASTE_STORAGE_AWS_IAM_USER_ARN_HERE"
         },
         "Action": "sts:AssumeRole",
         "Condition": {
           "StringEquals": {
             "sts:ExternalId": "PASTE_STORAGE_AWS_EXTERNAL_ID_HERE"
           }
         }
       }
     ]
   }
   ```

4. **Replace Placeholders**
   - Replace `PASTE_STORAGE_AWS_IAM_USER_ARN_HERE` with the ARN from Snowflake
   - Replace `PASTE_STORAGE_AWS_EXTERNAL_ID_HERE` with the External ID from Snowflake
   - **Important:** Keep the quotes around both values

5. **Save Changes**
   - Click "Update policy"
   - The trust relationship is now established

## Part D: Verification

### Step 6: Verify Integration in Snowflake

1. **Show Storage Integrations**
   ```sql
   SHOW STORAGE INTEGRATIONS;
   ```

   You should see your `S3_INTEGRATION` listed with:
   - Name: `S3_INTEGRATION`
   - Type: `EXTERNAL_STAGE`
   - Enabled: `true`

2. **Verify Role ARN**
   ```sql
   DESC STORAGE INTEGRATION s3_integration;
   ```

   Check that `STORAGE_AWS_ROLE_ARN` matches your IAM role ARN.

**Success Indicators:**
- ✅ No errors when running SQL commands
- ✅ Storage integration shows as `ENABLED = true`
- ✅ Trust policy in AWS matches Snowflake's IAM user

## Troubleshooting

### Common Issues

**Issue: "Insufficient privileges to operate on STORAGE INTEGRATION"**
- **Solution:** Ensure you're using `ACCOUNTADMIN` role in Snowflake
- Run: `USE ROLE ACCOUNTADMIN;` before creating the integration

**Issue: "Invalid parameter STORAGE_AWS_ROLE_ARN"**
- **Solution:** Double-check the ARN format
- Should be: `arn:aws:iam::123456789012:role/snowflake-s3-integration-role`
- No extra spaces or quotes

**Issue: Trust policy update fails in AWS**
- **Solution:** Ensure you have IAM permissions to modify trust relationships
- Need `iam:UpdateAssumeRolePolicy` permission

**Issue: External stage fails later with "Access Denied"**
- **Solution:** Trust policy not updated correctly
- Verify ARN and External ID match exactly between Snowflake and AWS
- Check for typos (common mistake: copying partial ARN)

## Security Best Practices

1. **Least Privilege**
   - IAM policy only grants read access to specific S3 bucket
   - No write or delete permissions
   - Cannot access other AWS resources

2. **No Stored Credentials**
   - No AWS access keys in Snowflake
   - Temporary STS tokens generated on-demand
   - Tokens expire automatically

3. **External ID Protection**
   - Prevents "confused deputy" attacks
   - Acts as a shared secret between Snowflake and AWS
   - Required for AssumeRole to succeed

4. **Audit Trail**
   - All S3 access logged in AWS CloudTrail (if enabled)
   - Snowflake query history shows when integration is used
   - IAM role usage visible in AWS

## What's Next

After completing this setup:
1. Proceed to **Step 6**: Create Snowflake database and schema architecture
2. In **Step 7**: Create external stages using this storage integration
3. Test S3 access by listing files with `LIST @stage_name`

The integration is now ready but won't be used until external stages are created in Step 7.

## Architecture Diagram

```
┌─────────────────────┐
│   Snowflake         │
│                     │
│  Storage Integration│
│  s3_integration     │
└──────────┬──────────┘
           │
           │ Uses IAM Role (AssumeRole)
           │
           ▼
┌─────────────────────┐
│   AWS IAM           │
│                     │
│  Role:              │
│  snowflake-s3-...   │
│                     │
│  Policy:            │
│  snowflake-s3-...   │
└──────────┬──────────┘
           │
           │ Read Permissions
           │
           ▼
┌─────────────────────┐
│   AWS S3            │
│                     │
│  Bucket:            │
│  your-bucket-name   │
│                     │
│  raw-data/          │
│  ├─ online_retail...│
│  └─ online_retail...│
└─────────────────────┘
```

## Reference Links

- [Snowflake: Configuring Secure Access to Cloud Storage](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [AWS: IAM Roles](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
- [AWS: STS AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
- [AWS: Using External IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)

---

**Time Estimate:** 30-45 minutes

**Difficulty:** Medium (requires understanding of AWS IAM and cross-account access)
