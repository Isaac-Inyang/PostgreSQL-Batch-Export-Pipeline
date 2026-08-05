# Pipeline Architecture

## Objective

Extract Olympic Games data from PostgreSQL and export it efficiently using batch processing.

## Pipeline Stages

### 1. Workspace Cleanup

Delete previous export files.

### 2. Connect to PostgreSQL

Establish database connection using SQLAlchemy.

### 3. Stream Query Results

Read records using yield_per() and fetchmany().

### 4. Batch Processing

Convert each batch into a Pandas DataFrame.

### 5. Export

Write batches incrementally to CSV or Parquet.

### 6. Validation

Compare exported row count against database row count.

### 7. Performance Metrics

Measure execution time and throughput.
