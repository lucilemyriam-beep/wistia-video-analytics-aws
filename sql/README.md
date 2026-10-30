# AWS Glue PySpark — Bronze to Silver

## Deployed Glue job

**AWS Glue job name:** `wistia-bronze-to-silver`

**Source code:** [`wistia_bronze_to_silver.py`](wistia_bronze_to_silver.py)

## Purpose

This AWS Glue PySpark job processes Wistia video analytics data from the Bronze layer in Amazon S3 and transforms it into structured data for the Silver layer.

## Processing workflow

1. Read Wistia JSON records from the S3 Bronze layer.
2. Extract video metadata from the JSON records.
3. Clean and standardize the data.
4. Transform records using PySpark.
5. Write processed data to the S3 Silver layer in Parquet format.

## AWS services

- AWS Glue
- Amazon S3
- Apache Spark / PySpark

## Current limitations

The project has experienced a Parquet schema compatibility issue when loading Silver data into Amazon Redshift. Successful execution of the Glue job does not independently establish that all downstream Redshift fact tables have been populated.

## Verification

The Python source file was copied from the deployed AWS Glue job and committed to this repository as part of FR12 documentation.
