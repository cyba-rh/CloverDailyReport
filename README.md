# CloverDaily Data Processing Pipeline

Automated pipeline for extracting, processing, and aggregating Clover transaction data.

## Overview

This project provides an end-to-end solution for:
1. Extracting transaction data from MySQL via SSH proxy
2. Uploading data to SQL Server
3. Aggregating daily transaction metrics
4. Tracking processing history and incremental updates

## Files

### Python Scripts
- **`mysql_ssh_export.py`** - Extracts data from MySQL database via SSH tunnel and exports to CSV
- **`upload_to_sqlserver.py`** - Uploads CSV data to SQL Server with data type conversion and error handling

### SQL Scripts
- **`create_cloverdaily_table.sql`** - Creates the CLOVERDAILY aggregation table
- **`aggregate_cloverdaily.sql`** - Full aggregation script (initial load)
- **`update_cloverdaily_incremental.sql`** - Incremental daily update script

### Configuration
- **`requirements.txt`** - Python dependencies
- **`.env.example`** - Template for environment variables
- **`.gitignore`** - Git ignore rules (excludes sensitive files)

## Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials
   ```

3. **Run initial SQL table creation:**
   ```sql
   -- Execute create_cloverdaily_table.sql in SQL Server
   ```

## Usage

### Daily Processing Workflow

1. **Extract data from MySQL:**
   ```bash
   python mysql_ssh_export.py
   ```

2. **Upload to SQL Server:**
   ```bash
   python upload_to_sqlserver.py
   ```

3. **Run aggregation:**
   ```sql
   -- For initial load: aggregate_cloverdaily.sql
   -- For daily updates: update_cloverdaily_incremental.sql
   ```

## Features

### Data Extraction
- SSH tunnel connection to MySQL
- Incremental extraction based on last run timestamp
- Automatic CSV file timestamping
- Error handling and retry logic

### Data Upload
- Batch processing for performance
- Automatic data type conversion
- Comprehensive error handling
- Progress tracking

### Data Aggregation
- Daily transaction summaries by merchant
- Transaction count metrics
- Running MTD and YTD totals
- First transaction date tracking
- Business transaction thresholds ($10+)

## Environment Variables

### MySQL Connection (Source)
- `SSH_HOST` - SSH proxy server
- `SSH_USER` - SSH username
- `SSH_KEY_PATH` - Path to SSH private key
- `DB_HOST` - MySQL database host
- `DB_USER` - MySQL username
- `DB_PASSWORD` - MySQL password
- `DB_NAME` - MySQL database name

### SQL Server Connection (Target)
- `SQLSERVER_HOST` - SQL Server host
- `SQLSERVER_USER` - SQL Server username
- `SQLSERVER_PASSWORD` - SQL Server password
- `SQLSERVER_DB` - SQL Server database name

### Processing Options
- `TRACKING_FILE` - Processing history file
- `CSV_OUTPUT` - Output file naming pattern
- `ARCHIVE_DIR` - Processed file storage

## Data Schema

### CLOVERTRANS (Source)
Transaction-level data with fields:
- `transaction_uuid` (unique key)
- `merchant_account_id`, `merchant_name`, `mid`
- `amount`, `refunded_amount` (in cents)
- `status`, `transaction_date`
- Additional metadata fields

### CLOVERDAILY (Target)
Daily aggregated data with fields:
- `day` - Transaction date
- `mid` - Merchant ID
- `merchant_name` - Merchant name
- `amount_sum`, `amount_count` - Total amounts and transaction counts
- `refund_sum`, `refund_count` - Total refunds and refund counts  
- `total` - Net amount (amount_sum - refund_sum)
- `first_transaction_date` - First ever transaction for merchant
- `first_business_transaction_date` - First $10+ transaction for merchant
- `mtd_total`, `ytd_total` - Running month and year totals

## Security Notes

- All credentials are stored in `.env` (excluded from git)
- SSH key-based authentication for database connection
- No SSL for SQL Server (configured for internal network)
- Sensitive files automatically ignored by git

## Monitoring

The pipeline includes:
- Processing status tracking
- Error logging and recovery
- Progress indicators for large datasets
- Summary reporting after each run