-- Milestone 1: normalized EVM ingestion facts and pipeline bookkeeping.

CREATE DATABASE IF NOT EXISTS chainlens;

-- Blocks are commonly read by chain and block range. ReplacingMergeTree allows a
-- later ingestion of the same logical block to supersede an earlier row (for
-- example after a reorg) once background merges occur. Queries that require an
-- immediate single version should resolve by max(ingested_at), not default to FINAL.
CREATE TABLE IF NOT EXISTS chainlens.raw_blocks
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    transaction_count UInt32,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (chain_id, block_number, block_hash);

-- The sorting key is the logical transaction identity. It makes transaction hash
-- lookups efficient and gives replacement semantics to idempotent re-ingestion.
-- A single unpartitioned layout is deliberate for the initial local data volume.
CREATE TABLE IF NOT EXISTS chainlens.raw_transactions
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash String,
    tx_index UInt32,
    from_address String,
    to_address Nullable(String),
    value_wei UInt256,
    gas UInt64,
    gas_price Nullable(UInt256),
    input String,
    status Nullable(UInt8),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (chain_id, block_number, block_hash, tx_index, tx_hash);

-- Transfer events are uniquely located by transaction hash and log index. Keeping
-- that identity in ORDER BY enables idempotent replacement and efficient event or
-- transaction-level retrieval without creating many small local partitions.
CREATE TABLE IF NOT EXISTS chainlens.token_transfers
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash String,
    log_index UInt32,
    token_address String,
    from_address String,
    to_address String,
    amount_raw UInt256,
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (chain_id, block_number, block_hash, tx_hash, log_index);


CREATE TABLE chainlens.canonical_blocks
(
    chain_id UInt64,
    block_number UInt64,

    block_hash String,
    parent_hash String,

    security_level Enum8(
        'unsafe' = 1,
        'safe' = 2,
        'finalized' = 3
    ),

    observed_at DateTime64(3, 'UTC'),

    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (chain_id, block_number);


-- Pipeline runs are a small operational history. MergeTree keeps every run, while
-- job/chain/time ordering supports recent-run inspection without partition churn.
CREATE TABLE IF NOT EXISTS chainlens.pipeline_runs
(
    run_id UUID,
    job_name LowCardinality(String),
    chain_id UInt64,
    mode Enum8('backfill' = 1, 'live' = 2),
    start_block Nullable(UInt64),
    end_block Nullable(UInt64),
    status Enum8(
        'pending' = 1,
        'running' = 2,
        'completed' = 3,
        'failed' = 4
    ),
    started_at DateTime64(3, 'UTC'),
    completed_at Nullable(DateTime64(3, 'UTC')),
    error Nullable(String)
)
ENGINE = MergeTree
ORDER BY (job_name, chain_id, started_at, run_id);

-- Checkpoints are updated by inserting a higher version. ReplacingMergeTree uses
-- version to retain the newest state for each job/chain during background merges;
-- readers needing immediate consistency can select argMax(..., version).
CREATE TABLE IF NOT EXISTS chainlens.pipeline_checkpoints
(
    job_name LowCardinality(String),
    chain_id UInt64,
    last_processed_block UInt64,
    last_processed_block_hash String
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (job_name, chain_id);

