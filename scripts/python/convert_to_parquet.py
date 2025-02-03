#!/usr/bin/env python3
"""
Convert Online Retail II dataset from CSV to Parquet format.

This script implements an ELT (Extract, Load, Transform) approach:
- Loads raw CSV data without cleaning
- Converts to Parquet format for efficient columnar storage
- Data quality issues will be handled in Snowflake STAGING_LAYER

Author: Data Engineering Portfolio
Date: 2025-02-03
"""

import os
import sys
import pandas as pd
from pathlib import Path


def get_file_size_mb(file_path: Path) -> float:
    """Get file size in megabytes."""
    return file_path.stat().st_size / (1024 * 1024)


def convert_csv_to_parquet(
    csv_path: Path,
    parquet_path: Path,
    chunk_size: int = 100000
) -> None:
    """
    Convert CSV file to Parquet format.

    Args:
        csv_path: Path to input CSV file
        parquet_path: Path to output Parquet file
        chunk_size: Number of rows to process at a time
    """
    print(f"Reading CSV file: {csv_path}")
    print(f"File size: {get_file_size_mb(csv_path):.2f} MB\n")

    try:
        # Read CSV file with pandas
        # Note: Not doing any data cleaning here - raw data quality issues
        # will be handled in Snowflake transformations
        df = pd.read_csv(
            csv_path,
            encoding='utf-8',
            parse_dates=['InvoiceDate']  # Parse dates for better Parquet compression
        )

        print(f"Dataset loaded successfully")
        print(f"Rows: {len(df):,}")
        print(f"Columns: {len(df.columns)}")
        print(f"\nColumn names:")
        for col in df.columns:
            print(f"  - {col}")

        # Display basic stats
        print(f"\nBasic statistics:")
        print(f"  Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
        print(f"  Unique invoices: {df['Invoice'].nunique():,}")
        print(f"  Unique products: {df['StockCode'].nunique():,}")
        print(f"  Unique customers: {df['Customer ID'].nunique():,}")

        # Create output directory if needed
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to Parquet format
        print(f"\nConverting to Parquet format...")
        df.to_parquet(
            parquet_path,
            engine='pyarrow',
            compression='snappy',  # Good balance of speed and compression
            index=False
        )

        print(f"Parquet file created: {parquet_path}")
        print(f"File size: {get_file_size_mb(parquet_path):.2f} MB")

        # Calculate compression ratio
        csv_size = get_file_size_mb(csv_path)
        parquet_size = get_file_size_mb(parquet_path)
        compression_ratio = (1 - parquet_size / csv_size) * 100

        print(f"\n{'='*60}")
        print(f"CONVERSION SUMMARY")
        print(f"{'='*60}")
        print(f"CSV size:          {csv_size:>10.2f} MB")
        print(f"Parquet size:      {parquet_size:>10.2f} MB")
        print(f"Compression ratio: {compression_ratio:>10.1f}%")
        print(f"Rows processed:    {len(df):>10,}")
        print(f"{'='*60}")

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error during conversion: {e}")
        sys.exit(1)


def main():
    """Main execution function."""
    # Define paths
    project_root = Path(__file__).parent.parent.parent
    csv_path = project_root / "data" / "raw" / "online_retail_II.csv"
    parquet_path = project_root / "data" / "processed" / "online_retail.parquet"

    print("="*60)
    print("CSV to Parquet Conversion Tool")
    print("Online Retail II Dataset")
    print("="*60)
    print()

    # Run conversion
    convert_csv_to_parquet(csv_path, parquet_path)

    print("\nConversion completed successfully!")
    print(f"\nNext steps:")
    print(f"  1. Review the Parquet file: {parquet_path}")
    print(f"  2. Upload both CSV and Parquet files to S3")
    print(f"  3. See docs/s3-upload-instructions.md for upload commands")


if __name__ == "__main__":
    main()
