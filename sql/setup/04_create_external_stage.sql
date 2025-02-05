/*******************************************************************************
 * Script: 04_create_external_stage.sql
 * Purpose: Create external stage pointing to AWS S3 for data ingestion
 *
 * Description:
 *   External stages in Snowflake act as pointers to external storage locations
 *   (S3, Azure Blob, GCS). They define WHERE data files are located and HOW
 *   to access them. Once created, stages simplify COPY INTO commands by
 *   eliminating the need to specify full S3 paths and credentials repeatedly.
 *
 *   This script creates the S3_ECOMMERCE_STAGE that references our S3 bucket
 *   configured in previous steps. The stage uses the storage integration for
 *   secure, credential-less authentication.
 *
 * Prerequisites:
 *   1. Storage integration configured (01_storage_integration.sql)
 *   2. Database and schemas created (02_create_database_schemas.sql)
 *   3. File formats defined (03_create_file_formats.sql)
 *   4. S3 bucket populated with data files
 *
 * Security Note:
 *   This stage uses STORAGE_INTEGRATION for authentication rather than
 *   embedding AWS credentials directly. This is the recommended secure
 *   approach as it leverages IAM roles and avoids credential management.
 *
 * Execution Instructions:
 *   1. Update the URL parameter with your actual S3 bucket name and prefix
 *   2. Verify storage integration exists: SHOW INTEGRATIONS;
 *   3. Execute this script in Snowflake worksheet
 *   4. Verify stage creation: SHOW STAGES IN SCHEMA ECOMMERCE_DW.RAW;
 *   5. Test stage access: LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE;
 *
 * Author: Andre Sbrocco
 * Created: 2025-02-04
 * Version: 1.0
 ******************************************************************************/

-- Set context to the appropriate database and schema
USE DATABASE ECOMMERCE_DW;
USE SCHEMA RAW;
USE ROLE ACCOUNTADMIN;

/*******************************************************************************
 * EXTERNAL STAGE CREATION
 *
 * Component Explanation:
 * ---------------------
 * URL = 's3://your-bucket-name/raw-data/'
 *   The S3 bucket location where data files are stored. This should match
 *   the bucket and prefix configured during AWS setup. The trailing slash
 *   is important - it indicates this is a directory/prefix path.
 *
 *   IMPORTANT: Replace 'your-bucket-name' with your actual S3 bucket name.
 *   Example: 's3://snowflake-ecommerce-data-andresbrocco/raw-data/'
 *
 * STORAGE_INTEGRATION = S3_INTEGRATION
 *   References the storage integration object created in step 1. This
 *   integration contains the IAM role ARN and trust relationship that
 *   allows Snowflake to access your S3 bucket without embedding credentials.
 *
 *   Benefits of using STORAGE_INTEGRATION:
 *   - No hardcoded AWS keys in SQL scripts
 *   - Credentials managed by AWS IAM, not Snowflake
 *   - Easier credential rotation (just update IAM, no SQL changes)
 *   - Follows AWS security best practices
 *
 * FILE_FORMAT = CSV_FORMAT
 *   Default file format to use when loading from this stage. By specifying
 *   a default format, COPY INTO commands become simpler - they inherit this
 *   format unless explicitly overridden.
 *
 *   You can still override this on a per-load basis:
 *   COPY INTO table FROM @stage FILE_FORMAT = (FORMAT_NAME = 'OTHER_FORMAT')
 *
 * Why Create Stages?
 * -----------------
 * Without a stage, each COPY command would need to specify:
 *   - Full S3 URL
 *   - AWS credentials or storage integration reference
 *   - File format details
 *
 * With a stage, COPY commands become much simpler:
 *   COPY INTO table FROM @stage_name;
 *
 * This reduces errors, improves maintainability, and centralizes
 * configuration. If the S3 path changes, you update the stage definition
 * once rather than updating dozens of COPY commands.
 ******************************************************************************/

CREATE OR REPLACE STAGE ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE
  URL = 's3://snowflake-ecommerce-data-andresbrocco/raw-data/'
  STORAGE_INTEGRATION = S3_INTEGRATION
  FILE_FORMAT = ECOMMERCE_DW.RAW.CSV_FORMAT
  COMMENT = 'External stage pointing to S3 bucket containing e-commerce CSV and Parquet files';

-- Confirm stage creation
SELECT 'S3_ECOMMERCE_STAGE created successfully' AS status;

/*******************************************************************************
 * VERIFICATION QUERIES
 ******************************************************************************/

-- Display all stages in the RAW schema
SHOW STAGES IN SCHEMA ECOMMERCE_DW.RAW;

-- Display detailed properties of the stage
DESC STAGE ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE;

/*******************************************************************************
 * STAGE TESTING
 *
 * These commands verify that the stage is correctly configured and can
 * access files in S3. If these queries fail, check:
 *   1. Storage integration is properly configured
 *   2. IAM role has permission to read from S3 bucket
 *   3. S3 bucket name and prefix are correct
 *   4. Trust relationship is established between Snowflake and IAM role
 ******************************************************************************/

-- List all files accessible through this stage
-- This should return the CSV and Parquet files you uploaded to S3
LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE;

-- List only CSV files (using pattern matching)
LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN = '.*\\.csv';

-- List only Parquet files (using pattern matching)
LIST @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE PATTERN = '.*\\.parquet';

/*******************************************************************************
 * TROUBLESHOOTING TIPS
 *
 * If LIST command returns no results:
 * ----------------------------------
 * 1. Verify files exist in S3:
 *    - Log into AWS Console
 *    - Navigate to your S3 bucket
 *    - Check the raw-data/ prefix contains files
 *
 * 2. Verify storage integration is working:
 *    DESC INTEGRATION S3_INTEGRATION;
 *    - Check STORAGE_AWS_IAM_USER_ARN matches IAM trust policy
 *    - Check STORAGE_AWS_EXTERNAL_ID matches IAM trust policy
 *
 * 3. Verify IAM role has correct permissions:
 *    - IAM role should have s3:GetObject and s3:ListBucket permissions
 *    - Bucket policy should allow access from the IAM role
 *
 * 4. Verify URL path is correct:
 *    - Check bucket name spelling
 *    - Check prefix/folder structure matches S3
 *    - Ensure trailing slash is present for directories
 *
 * If LIST command returns "Access Denied" error:
 * --------------------------------------------
 * - IAM role trust relationship may be incorrect
 * - Check STORAGE_AWS_IAM_USER_ARN in storage integration
 * - Check STORAGE_AWS_EXTERNAL_ID in storage integration
 * - Verify IAM role trust policy allows Snowflake to assume role
 *
 * For detailed error messages:
 * --------------------------
 * SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
 ******************************************************************************/

/*******************************************************************************
 * USAGE EXAMPLES
 *
 * Once the stage is created and verified, you can use it in COPY commands:
 *
 * Example 1: Load CSV file using default stage format
 * ---------------------------------------------------
 * COPY INTO ECOMMERCE_DW.RAW.raw_transactions
 * FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/online_retail.csv;
 *
 * Example 2: Load Parquet file (override default format)
 * ------------------------------------------------------
 * COPY INTO ECOMMERCE_DW.RAW.raw_transactions
 * FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/online_retail.parquet
 * FILE_FORMAT = (FORMAT_NAME = 'ECOMMERCE_DW.RAW.PARQUET_FORMAT');
 *
 * Example 3: Load all CSV files in a subdirectory
 * -----------------------------------------------
 * COPY INTO ECOMMERCE_DW.RAW.raw_transactions
 * FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/
 * PATTERN = '.*\\.csv';
 *
 * Example 4: Load with pattern matching and error handling
 * --------------------------------------------------------
 * COPY INTO ECOMMERCE_DW.RAW.raw_transactions
 * FROM @ECOMMERCE_DW.RAW.S3_ECOMMERCE_STAGE/
 * PATTERN = '.*transactions.*\\.csv'
 * ON_ERROR = 'CONTINUE'
 * RETURN_FAILED_ONLY = TRUE;
 *
 * Stage Benefits Summary:
 * ----------------------
 * ✓ Centralized S3 configuration
 * ✓ Simplified COPY commands
 * ✓ Secure credential-less authentication
 * ✓ Reusable across multiple tables and loads
 * ✓ Easy to update if S3 path changes
 *
 * Next Steps:
 * ----------
 * 1. Create RAW layer tables to receive data (sql/raw/01_create_raw_table.sql)
 * 2. Load data from stage into tables (sql/raw/02_load_data_csv.sql)
 * 3. Benchmark CSV vs Parquet performance (sql/raw/04_benchmark_csv_vs_parquet.sql)
 ******************************************************************************/
