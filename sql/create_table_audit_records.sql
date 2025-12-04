-- Create database audit_logs (if not exists)
-- CREATE DATABASE IF NOT EXISTS audit_logs;

-- Create common audit_records table for all audit data
-- Partition by month (from sync_date) and table
-- TTL: Automatically delete data after 6 months

CREATE TABLE IF NOT EXISTS audit_records 
(
    -- Primary identifier
    id String,
    
    -- Kafka metadata
    topic String,
    partition UInt32,
    offset Nullable(Int64),
    
    -- Change information
    change_type String,
    change_time DateTime64(3),
    sync_date DateTime64(3),
    
    -- Data fields
    primary_keys String,
    new_values String,
    original_values String,
    field_includes String,
    
    -- Source database information
    database String,
    schema Nullable(String),
    table String,
    
    -- CDC source metadata
    connector Nullable(String),
    change_lsn Nullable(String),
    commit_lsn Nullable(String),
    event_serial_no Nullable(Int64),
    snapshot Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(sync_date), table)
ORDER BY (table, sync_date)
TTL sync_date + INTERVAL 6 MONTH;

