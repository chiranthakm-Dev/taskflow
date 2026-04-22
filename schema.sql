-- Database schema for taskflow
-- Run this against your PostgreSQL database before starting the services

CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    priority VARCHAR(50) NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    payload JSONB NOT NULL,
    max_retries INTEGER NOT NULL DEFAULT 3,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'dead')),
    idempotency_key VARCHAR(255) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    processing_time_ms BIGINT
);

-- Index for idempotency key lookups
CREATE INDEX IF NOT EXISTS idx_jobs_idempotency_key ON jobs(idempotency_key);

-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- Index for created_at for cleanup/archival
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);