
-- FR12: Wistia Video Analytics
-- Redshift table definitions reconstructed from verified column metadata.
-- Column lengths, numeric precision, defaults, and constraints
-- must be checked against the original deployed DDL.
-- Documentation only: do not run against existing tables.

CREATE TABLE IF NOT EXISTS public.dim_media (
    media_id BIGINT,
    hashed_id VARCHAR(65535),
    title VARCHAR(65535),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    duration_seconds NUMERIC
);

CREATE TABLE IF NOT EXISTS public.dim_visitor (
    visitor_id VARCHAR(65535),
    ip_address VARCHAR(65535),
    first_seen_at TIMESTAMP,
    last_seen_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.fact_media_engagement (
    engagement_id BIGINT,
    media_hashed_id VARCHAR(65535),
    visitor_id VARCHAR(65535),
    event_type VARCHAR(65535),
    event_timestamp TIMESTAMP,
    play_count INTEGER,
    play_rate NUMERIC,
    hours_watched NUMERIC,
    engagement NUMERIC,
    load_count INTEGER,
    ingestion_date DATE
);
