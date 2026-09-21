-- Milestone 3: bounded, explainable wallet-intelligence snapshots.

CREATE TABLE IF NOT EXISTS chainlens.wallet_transfer_edges
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    tx_hash String,
    asset_type Enum8('native' = 1, 'erc20' = 2),
    token_address Nullable(String),
    from_address String,
    to_address String,
    amount_raw UInt256,
    edge_id String,
    derived_at DateTime64(3, 'UTC'),
    run_id UUID
)
ENGINE = MergeTree
ORDER BY (chain_id, block_number, from_address, to_address, edge_id, run_id);

CREATE TABLE IF NOT EXISTS chainlens.wallet_funding_edges
(
    chain_id UInt64,
    funded_address String,
    funder_address String,
    funding_tx_hash String,
    funding_block_number UInt64,
    funding_block_hash String,
    funding_timestamp DateTime64(3, 'UTC'),
    amount_wei UInt256,
    heuristic LowCardinality(String),
    confidence Float32,
    run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, funded_address, funding_block_number, funder_address, run_id);

CREATE TABLE IF NOT EXISTS chainlens.wallet_features
(
    chain_id UInt64,
    wallet_address String,
    first_seen_block UInt64,
    last_seen_block UInt64,
    incoming_native_tx_count UInt64,
    outgoing_native_tx_count UInt64,
    incoming_erc20_transfer_count UInt64,
    outgoing_erc20_transfer_count UInt64,
    unique_counterparties UInt64,
    native_received_wei UInt256,
    native_sent_wei UInt256,
    first_funder_address Nullable(String),
    first_funding_block Nullable(UInt64),
    run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, wallet_address, run_id);

CREATE TABLE IF NOT EXISTS chainlens.entity_resolution_edges
(
    chain_id UInt64,
    wallet_a String,
    wallet_b String,
    heuristic LowCardinality(String),
    score Float32,
    evidence_count UInt32,
    run_id UUID,
    derived_at DateTime64(3, 'UTC'),
    edge_id String
)
ENGINE = MergeTree
ORDER BY (chain_id, wallet_a, wallet_b, heuristic, edge_id, run_id);

CREATE TABLE IF NOT EXISTS chainlens.cluster_evidence
(
    chain_id UInt64,
    wallet_a String,
    wallet_b String,
    heuristic LowCardinality(String),
    evidence_type LowCardinality(String),
    evidence_value String,
    weight Float32,
    score_contribution Float32,
    run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, wallet_a, wallet_b, heuristic, evidence_type, run_id);

-- State transitions append a higher version; no routine UPDATE mutation is used.
CREATE TABLE IF NOT EXISTS chainlens.cluster_runs
(
    run_id UUID,
    chain_id UInt64,
    start_block UInt64,
    end_block UInt64,
    heuristic_version String,
    status Enum8('pending' = 1, 'running' = 2, 'completed' = 3, 'failed' = 4),
    started_at DateTime64(3, 'UTC'),
    completed_at Nullable(DateTime64(3, 'UTC')),
    wallet_count UInt64,
    resolution_edge_count UInt64,
    cluster_count UInt64,
    error Nullable(String),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id);

CREATE TABLE IF NOT EXISTS chainlens.wallet_clusters
(
    chain_id UInt64,
    cluster_id String,
    wallet_address String,
    cluster_run_id UUID,
    cluster_size UInt32,
    confidence Float32,
    heuristic_version String,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, cluster_id, wallet_address, cluster_run_id);
