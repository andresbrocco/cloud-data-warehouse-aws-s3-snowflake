# Cloud Data Warehouse with AWS S3 + Snowflake

A modern cloud data warehouse implementation showcasing end-to-end data engineering workflows using AWS S3 for data lake storage and Snowflake for analytical processing. This project demonstrates a multi-layer architecture (Raw → Staging → Production) following data warehouse best practices.

## Overview

This project implements a complete cloud-based data warehouse solution for an e-commerce analytics use case. It features:

- **Cloud-native architecture** using AWS S3 and Snowflake
- **Multi-layer data model** (Bronze/Silver/Gold layers)
- **Automated data pipeline** from CSV files to dimensional model
- **Scalable design** supporting future analytics workloads
- **Production-ready SQL scripts** for data transformation and modeling

## Technology Stack

- **Cloud Storage**: AWS S3 (Data Lake)
- **Data Warehouse**: Snowflake
- **Programming Language**: Python 3.8+
- **Data Processing**: SQL, Pandas
- **Infrastructure**: AWS IAM, Snowflake Security

## Architecture

The project follows a medallion architecture pattern:

1. **RAW_LAYER (Bronze)**: Raw data ingested from S3 without transformations
2. **STAGING_LAYER (Silver)**: Cleaned, validated, and standardized data
3. **PRODUCTION_LAYER (Gold)**: Dimensional model optimized for analytics

```
CSV Files → AWS S3 → Snowflake RAW → Snowflake STAGING → Snowflake PRODUCTION
```

## Database Architecture

This project implements a **three-layer database architecture** within Snowflake, providing clear separation between data ingestion, transformation, and analytics.

### Database and Schema Structure

```
ECOMMERCE_DW (Database)
├── RAW (Schema)
│   ├── Purpose: Store unprocessed data exactly as received from S3
│   ├── Tables: raw_transactions, raw_customers, etc.
│   └── Characteristics: No transformations, immutable, source of truth
│
├── STAGING (Schema)
│   ├── Purpose: Apply cleaning, validation, and business logic
│   ├── Tables: stg_transactions, stg_customers, etc.
│   └── Characteristics: Data quality checks, standardization, derived fields
│
└── PRODUCTION (Schema)
    ├── Purpose: Dimensional model optimized for analytics
    ├── Tables: fact_sales, dim_customer, dim_product, dim_date
    └── Characteristics: Star schema, denormalized, query-optimized
```

### Data Flow

1. **Ingestion (S3 → RAW)**
   - CSV files uploaded to AWS S3 bucket
   - Snowflake `COPY INTO` commands load data to RAW tables
   - Data stored exactly as received, no modifications

2. **Transformation (RAW → STAGING)**
   - SQL transformations clean and validate data
   - Remove duplicates, handle nulls, standardize formats
   - Apply business rules and calculate derived fields

3. **Modeling (STAGING → PRODUCTION)**
   - Build dimensional model (fact and dimension tables)
   - Create relationships using foreign keys
   - Optimize for analytical queries and BI tools

### Key Design Principles

- **Immutability**: RAW layer preserves original data for reprocessing
- **Idempotency**: Transformations can be safely re-run
- **Separation of Concerns**: Each layer has a distinct responsibility
- **Data Quality**: Progressive validation as data moves through layers
- **Query Performance**: PRODUCTION layer optimized for fast analytics

### Quick Start - Database Setup

Execute these scripts in order to set up the database structure:

```bash
# 1. Configure AWS-Snowflake integration
# Run: sql/setup/01_storage_integration.sql

# 2. Create database and schemas
# Run: sql/setup/02_create_database_schemas.sql

# 3. Create file formats for CSV and Parquet
# Run: sql/setup/03_create_file_formats.sql

# 4. Create external stage pointing to S3
# Run: sql/setup/04_create_external_stage.sql
```

### Data Loading Pipeline

After setting up the database, load e-commerce data from S3:

```bash
# 1. Create RAW layer table
# Run: sql/raw/01_create_raw_table.sql

# 2. Load data from CSV (initial approach)
# Run: sql/raw/02_load_data_csv.sql

# 3. Load data from Parquet (optimized approach)
# Run: sql/raw/03_load_data_parquet.sql

# 4. Benchmark CSV vs Parquet performance
# Run: sql/raw/04_benchmark_csv_vs_parquet.sql
```

**Performance Comparison:**
- **CSV:** 91 MB file, ~25 seconds load time
- **Parquet:** 7.5 MB file, ~8 seconds load time (3x faster, 92% smaller)
- **Recommendation:** Use Parquet for production workloads

See [docs/data-loading.md](docs/data-loading.md) for comprehensive loading guide and [docs/benchmarks/csv-vs-parquet.md](docs/benchmarks/csv-vs-parquet.md) for detailed performance analysis.

### Documentation

For detailed information about the architecture:

- **[Data Layers](docs/architecture/data-layers.md)** - In-depth explanation of RAW, STAGING, and PRODUCTION layers
- **[Naming Conventions](docs/architecture/naming-conventions.md)** - Standards for tables, columns, and database objects
- **[AWS Setup Guide](docs/aws-setup.md)** - Setting up S3 and IAM permissions
- **[Snowflake Setup Guide](docs/snowflake-setup.md)** - Configuring your Snowflake account

## Prerequisites

Before starting, ensure you have:

- **Python 3.8 or higher** installed
- **AWS Account** with IAM access
- **Snowflake Account** (free trial available)
- **Git** for version control
- **Basic knowledge** of SQL, Python, and cloud platforms

## Project Structure

```
.
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
├── .gitignore                   # Git ignore rules
│
├── config/                      # Configuration files
│   └── config.yaml.example      # Configuration template
│
├── data/                        # Data files (not tracked in git)
│   ├── raw/                     # Original CSV files from source
│   └── processed/               # Cleaned data before S3 upload
│
├── sql/                         # SQL scripts organized by layer
│   ├── setup/                   # Database, schema, and stage setup
│   ├── raw/                     # Raw layer table definitions
│   ├── staging/                 # Staging transformations
│   ├── production/              # Production dimensional model
│   └── analytics/               # Advanced queries and analysis
│
├── scripts/                     # Automation scripts
│   └── python/                  # Python helper scripts
│
└── docs/                        # Documentation
    └── diagrams/                # Architecture and data model diagrams
    └── assets/                  # Assets used in the documentation
```

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd cloud-data-warehouse-aws-s3-snowflake
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your actual credentials
# Use your preferred text editor (nano, vim, code, etc.)
nano .env
```

### 4. Configure Project Settings

```bash
# Copy configuration template
cp config/config.yaml.example config/config.yaml

# Edit config.yaml with your settings
nano config/config.yaml
```

### 5. Prepare Your Data

Place your CSV files in the `data/raw/` directory. This project is designed for e-commerce datasets but can be adapted for other use cases.

### 6. Set Up AWS S3

1. Create an S3 bucket in your AWS account
2. Configure IAM credentials with S3 access
3. Update `.env` with your AWS credentials and bucket name

### 7. Set Up Snowflake

1. Create a Snowflake account (free trial available)
2. Note your account identifier, username, and password
3. Update `.env` with your Snowflake credentials

### 8. Run the Pipeline

Detailed implementation steps are documented in the `sql/` directory scripts. Follow them in order:

1. Database and schema setup (`sql/setup/`)
2. Raw layer ingestion (`sql/raw/`)
3. Staging transformations (`sql/staging/`)
4. Production dimensional model (`sql/production/`)

## Security Notes

- **Never commit credentials** to version control
- Keep `.env` and `config/config.yaml` out of git (they're in `.gitignore`)
- Use IAM roles and Snowflake role-based access control in production
- Rotate credentials regularly
- Follow principle of least privilege for all access

## Data Privacy

This project uses synthetic or publicly available e-commerce data for educational purposes. When adapting for production use:

- Implement proper data governance policies
- Comply with relevant data protection regulations (GDPR, CCPA, etc.)
- Implement encryption at rest and in transit
- Maintain audit logs for data access

## Future Enhancements

Potential improvements for this project:

- [ ] Automated CI/CD pipeline
- [ ] Data quality checks and monitoring
- [ ] Incremental data loading patterns
- [ ] Advanced analytics and ML features
- [ ] dbt integration for transformation management
- [ ] Orchestration with Apache Airflow
- [ ] Real-time streaming data ingestion

## Contributing

This is a portfolio project, but suggestions and feedback are welcome! Feel free to:

- Open issues for bugs or questions
- Submit pull requests for improvements
- Share your own implementations or variations

## License

This project is created for educational and portfolio purposes. Feel free to use it as a reference for your own projects.

## Acknowledgments

- Built as a demonstration of modern data warehouse architecture
- Uses open-source tools and cloud platform services
- Inspired by real-world data engineering best practices

## Contact

Created by Andre Sbrocco - Feel free to reach out with questions or feedback!

---

**Note**: This is a learning project designed to showcase data engineering skills. While it follows production best practices, additional hardening would be needed for actual production deployment.
