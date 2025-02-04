-- ============================================================================
-- Storage Integration Setup for Snowflake-AWS S3 Access
-- ============================================================================
-- Purpose: Create a secure, passwordless connection between Snowflake and AWS S3
-- Security: Uses AWS IAM role with temporary STS tokens (no access keys stored)
-- Prerequisites: AWS IAM role created with proper trust relationship
-- ============================================================================

-- Step 1: Create Storage Integration
-- This tells Snowflake to use AWS IAM for authentication to S3
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR-ACCOUNT-ID:role/snowflake-s3-integration-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://YOUR-BUCKET-NAME/raw-data/');

-- Note: Replace the following placeholders before running:
--   YOUR-ACCOUNT-ID  → Your 12-digit AWS account ID (e.g., 123456789012)
--   YOUR-BUCKET-NAME → Your S3 bucket name (e.g., de-portfolio-snowflake-integration)

-- ============================================================================
-- Step 2: Retrieve Snowflake's IAM User Details
-- ============================================================================
-- Run this command to get the IAM user ARN and External ID
-- You'll need these values to update the AWS IAM role's trust policy

DESC STORAGE INTEGRATION s3_integration;

-- Important outputs from the above command:
-- 1. STORAGE_AWS_IAM_USER_ARN → Copy this value
--    Example: arn:aws:iam::123456789012:user/abc12345-s
-- 2. STORAGE_AWS_EXTERNAL_ID → Copy this value
--    Example: ABC12345_SFCRole=1_aBcDeFgHiJkLmNoPqRsTuVwXyZ=

-- ============================================================================
-- Step 3: Verify Integration Creation
-- ============================================================================
-- This should show your newly created storage integration
SHOW STORAGE INTEGRATIONS;

-- ============================================================================
-- Security Notes
-- ============================================================================
-- 1. External ID: Acts as a shared secret between Snowflake and AWS
-- 2. IAM Role: Only grants read-only access to specified S3 locations
-- 3. No Credentials: Snowflake never stores AWS access keys or secrets
-- 4. Temporary Tokens: AWS STS generates short-lived credentials on-demand
-- 5. Least Privilege: IAM policy restricts access to specific bucket/prefix

-- ============================================================================
-- Troubleshooting
-- ============================================================================
-- Error: "Insufficient privileges to operate on STORAGE INTEGRATION"
--   → Ensure you're using ACCOUNTADMIN role or have CREATE INTEGRATION privilege

-- Error: "Invalid parameter STORAGE_AWS_ROLE_ARN"
--   → Check ARN format (must be exactly from IAM role in AWS Console)

-- Error: "Access Denied" (later, when using external stage)
--   → IAM trust policy not updated with Snowflake IAM user ARN
--   → Check External ID matches in both Snowflake and AWS

-- ============================================================================
-- Next Steps
-- ============================================================================
-- 1. Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from DESC output
-- 2. Go to AWS IAM Console → Roles → snowflake-s3-integration-role
-- 3. Update trust relationship with Snowflake's IAM user details
-- 4. Proceed to Step 6: Create external stages using this integration
