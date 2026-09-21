-- Milestone 1 correction: fork-aware facts, canonicality/finality state, and
-- versioned operational state.
--
-- This local-development migration is intentionally destructive for the affected
-- tables. They contain no meaningful production data, and DROP/recreate makes the
-- engine and sorting-key changes explicit. ClickHouse DDL is not transactional, so
-- stop writers before applying this migration and rerun it if execution is
-- interrupted. The IF EXISTS/IF NOT EXISTS guards make reruns deterministic.

DROP VIEW IF EXISTS chainlens.canonical_token_transfers;
DROP VIEW IF EXISTS chainlens.canonical_transactions;
DROP VIEW IF EXISTS chainlens.current_canonical_blocks;

DROP TABLE IF EXISTS chainlens.pipeline_checkpoints;
DROP TABLE IF EXISTS chainlens.pipeline_runs;
DROP TABLE IF EXISTS chainlens.token_transfers;
DROP TABLE IF EXISTS chainlens.raw_transactions;
DROP TABLE IF EXISTS chainlens.raw_blocks;

-- Multiple block hashes at one height are distinct observed facts. Replacement
-- only collapses repeated ingestion of the same (chain, height, hash); it does not
-- decide which fork is canonical.
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

-- block_hash ties each transaction occurrence to its exact observed fork. The
-- physical key favors chain/block scans and deterministic transaction order; the
-- application identity is (chain_id, block_hash, tx_hash), not the ORDER BY tuple.
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

-- block_hash keeps events from orphaned blocks attributable to their observed
-- fork. The application identity is (chain_id, block_hash, tx_hash, log_index),
-- while the physical key is arranged for chain and block-range scans.
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

-- Current canonical selection and protocol security state. Writers append a higher
-- version for changes; immediate reads aggregate with argMax instead of using FINAL.
CREATE TABLE IF NOT EXISTS chainlens.canonical_blocks
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    security_level Enum8('unsafe' = 1, 'safe' = 2, 'finalized' = 3),
    observed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (chain_id, block_number);

-- Append-only audit trail of every canonicality and security-state decision.
CREATE TABLE IF NOT EXISTS chainlens.canonical_blocks_history
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    security_level Enum8('unsafe' = 1, 'safe' = 2, 'finalized' = 3),
    canonical UInt8,
    reason LowCardinality(String),
    detected_at DateTime64(3, 'UTC'),
    sequence UInt64
)
ENGINE = MergeTree
ORDER BY (chain_id, block_number, sequence);

-- Operational summary of a detected reorganization. Raw orphaned facts remain in
-- their source tables; this table does not duplicate or collapse those facts.
CREATE TABLE IF NOT EXISTS chainlens.reorg_events
(
    reorg_id UUID,
    chain_id UInt64,
    detected_at DateTime64(3, 'UTC'),
    detected_at_block UInt64,
    old_head_block Nullable(UInt64),
    old_head_hash Nullable(String),
    new_head_block UInt64,
    new_head_hash String,
    common_ancestor_block UInt64,
    common_ancestor_hash String,
    orphaned_block_count UInt32,
    replacement_block_count UInt32
)
ENGINE = MergeTree
ORDER BY (chain_id, detected_at, reorg_id);

-- Run transitions append a new, higher version for the same run_id. Ordinary
-- state changes do not use UPDATE mutations; current reads use argMax by version.
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
    error Nullable(String),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id);

-- A checkpoint includes the observed hash so resume logic can later detect that a
-- previously processed height is no longer canonical. Recovery is not implemented
-- in this milestone. Current reads aggregate with argMax by version.
CREATE TABLE IF NOT EXISTS chainlens.pipeline_checkpoints
(
    job_name LowCardinality(String),
    chain_id UInt64,
    last_processed_block UInt64,
    last_processed_block_hash String,
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (job_name, chain_id);

-- Resolve one logical current record per chain height without relying on merges.
CREATE VIEW IF NOT EXISTS chainlens.current_canonical_blocks AS
SELECT
    chain_id,
    block_number,
    tupleElement(current_state, 1) AS block_hash,
    tupleElement(current_state, 2) AS parent_hash,
    tupleElement(current_state, 3) AS security_level,
    tupleElement(current_state, 4) AS observed_at,
    max_version AS version
FROM
(
    SELECT
        chain_id,
        block_number,
        argMax(
            tuple(block_hash, parent_hash, security_level, observed_at),
            version
        ) AS current_state,
        max(version) AS max_version
    FROM chainlens.canonical_blocks
    GROUP BY chain_id, block_number
);

-- Canonical transaction reads match all fork-disambiguating block coordinates.
CREATE VIEW IF NOT EXISTS chainlens.canonical_transactions AS
SELECT transactions.*
FROM chainlens.raw_transactions AS transactions
INNER JOIN chainlens.current_canonical_blocks AS canonical
    ON transactions.chain_id = canonical.chain_id
    AND transactions.block_number = canonical.block_number
    AND transactions.block_hash = canonical.block_hash;

-- Canonical transfer reads use the same current block-hash association.
CREATE VIEW IF NOT EXISTS chainlens.canonical_token_transfers AS
SELECT transfers.*
FROM chainlens.token_transfers AS transfers
INNER JOIN chainlens.current_canonical_blocks AS canonical
    ON transfers.chain_id = canonical.chain_id
    AND transfers.block_number = canonical.block_number
    AND transfers.block_hash = canonical.block_hash;

