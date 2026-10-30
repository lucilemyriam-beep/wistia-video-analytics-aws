# Amazon Redshift SQL — Wistia Video Analytics

## Purpose

This folder contains the SQL scripts used to define, analyze, and validate the Amazon Redshift data warehouse for the Wistia Video Analytics project.

## SQL files

- `create_tables.sql` — Documents the Redshift table structures.
- `fr11_kpis.sql` — Contains analytics KPI queries used for reporting.
- `validation.sql` — Contains queries for validating warehouse data.

## Data warehouse tables

The project uses three main tables:

1. `dim_media` — Stores Wistia video metadata.
2. `dim_visitor` — Stores visitor-related information.
3. `fact_media_engagement` — Stores video engagement events and metrics.

## Current limitations

The Redshift table structures have been verified.

However, the most recent verification identified a Parquet schema compatibility issue during data loading. The engagement fact tables have not yet been verified as successfully populated.

## Verification

The repository includes SQL scripts for reviewing the Redshift schema, checking data quality, and generating analytics metrics.

Successful table creation does not independently confirm successful data loading.
