# E-Commerce Data Pipeline

An end-to-end batch data engineering pipeline that ingests raw e-commerce data, stores it in PostgreSQL and Amazon S3, transforms it using PySpark, and loads analytics-ready data into PostgreSQL.

## Project Overview

This project uses the [Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail) from the UCI Machine Learning Repository. It contains approximately 542,000 transaction records from a UK-based online retailer.

The pipeline demonstrates a modular batch ETL workflow, including data ingestion, validation, cloud storage, distributed data transformation, and analytics loading.

## Architecture

```text
Online Retail.xlsx
       |
       v
  Data Ingestion
   (Pandas)
       |
       v
 PostgreSQL
  Raw Schema
       |
       v
 Export Raw Data
     to CSV
       |
       v
 Amazon S3
  Raw Storage
       |
       v
 Download from S3
       |
       v
 PySpark Transformation
       |
       v
 Transformed CSV
       |
       v
 PostgreSQL
 Analytics Schema
```

## Technology Stack

- **Python** – Pipeline orchestration and data processing
- **Pandas** – Source file ingestion and CSV handling
- **PostgreSQL** – Raw and analytics data storage
- **PySpark** – Data transformation
- **Amazon S3** – Cloud-based raw data storage
- **Boto3** – AWS S3 integration
- **psycopg2** – PostgreSQL connectivity and bulk loading

## Pipeline Workflow

### 1. Data Ingestion
- Reads the source Excel dataset using Pandas.
- Validates the source columns against the expected schema.
- Creates the raw PostgreSQL table.
- Loads all source records into PostgreSQL using PostgreSQL's `COPY` command.

### 2. Raw Data Export and S3 Storage
- Exports raw PostgreSQL data into CSV format using batch fetching.
- Uploads the CSV file to an Amazon S3 bucket.
- Verifies the uploaded object using its metadata.
- Downloads the S3 object locally for the transformation stage.

### 3. Data Transformation
PySpark processes the downloaded raw CSV and applies the following rules:

- Removes duplicate records.
- Excludes cancelled invoices.
- Removes records with non-positive quantities.
- Removes records with non-positive unit prices.
- Calculates transaction revenue using quantity multiplied by unit price.

### 4. Analytics Loading
- Exports the transformed dataset to CSV.
- Creates the analytics table in PostgreSQL.
- Loads the transformed records into the analytics schema using `COPY`.
- Validates the loaded record count.

## Data Validation

The pipeline includes validation checks at different stages:

- Source file schema validation.
- Raw table existence and schema validation.
- Source-to-destination data consistency checks.
- CSV export record count validation.
- S3 upload and download file-size validation.
- Spark session and DataFrame validation.
- Transformation rule validation.
- Analytics table and loaded record count validation.

A validation failure stops the pipeline to prevent subsequent stages from running with invalid data.

## Project Structure

```text
ecommerce-data-pipeline/
│
├── config.py
├── database.py
├── ingestion.py
├── loading.py
├── transformation.py
├── validation.py
├── pipeline.py
│
├── data/
│   |── raw/
|   |     └── Online Retail.xlsx
│   └── staged/
|   |     └── online_retail.xlsx
│   └── transformed/
|         |── cloud_files/
|         |     └── online_retail.xlsx
|         └── online_retail.xlsx
|
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup and Execution

### Prerequisites

- Python
- PostgreSQL
- Java (for local PySpark execution)
- Apache Spark / PySpark
- AWS account with an S3 bucket
- AWS CLI configured with appropriate credentials

### 1. Clone the repository

```bash
git clone https://github.com/RohitBirhade/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

### 2. Create a virtual environment

```bash
python -m venv ecommerce-pipeline-env
```

Activate it on Windows:

```bash
ecommerce-pipeline-env\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure the environment

Update the configuration in `config.py` with your PostgreSQL connection details, local file paths, S3 bucket name, and S3 object key.

Ensure your AWS credentials are configured locally and have the required S3 permissions.

### 5. Run the pipeline

```bash
python pipeline.py
```

## Current Scope

This implementation performs a **full batch refresh**. Raw and analytics tables are refreshed during each execution.

Incremental loading,