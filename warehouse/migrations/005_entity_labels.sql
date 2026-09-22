-- Milestone 4: immutable, provenance-first entity and label snapshots.

CREATE TABLE IF NOT EXISTS chainlens.label_taxonomy
(
    taxonomy_version String,
    label_dimension LowCardinality(String),
    label_value String,
    description String,
    active UInt8,
    created_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (taxonomy_version, label_dimension, label_value);

INSERT INTO chainlens.label_taxonomy
SELECT taxonomy_version, label_dimension, label_value, description, active, now64(3)
FROM values(
    'taxonomy_version String, label_dimension String, label_value String, description String, active UInt8',
    ('taxonomy_v1', 'entity_category', 'dex', 'Decentralized exchange or liquidity protocol', 1),
    ('taxonomy_v1', 'entity_category', 'exchange', 'Centralized exchange entity', 1),
    ('taxonomy_v1', 'entity_category', 'bridge', 'Cross-chain bridge entity', 1),
    ('taxonomy_v1', 'entity_category', 'protocol', 'Protocol not covered by a narrower category', 1),
    ('taxonomy_v1', 'entity_category', 'token_issuer', 'Token issuing organization', 1),
    ('taxonomy_v1', 'entity_category', 'unknown', 'No supported entity category is established', 1),
    ('taxonomy_v1', 'contract_role', 'router', 'Transaction routing contract', 1),
    ('taxonomy_v1', 'contract_role', 'factory', 'Contract factory', 1),
    ('taxonomy_v1', 'contract_role', 'pool_manager', 'Liquidity pool manager', 1),
    ('taxonomy_v1', 'contract_role', 'position_manager', 'Liquidity position manager', 1),
    ('taxonomy_v1', 'contract_role', 'token_contract', 'Token contract', 1),
    ('taxonomy_v1', 'contract_role', 'protocol_contract', 'Other protocol-specific contract', 1),
    ('taxonomy_v1', 'contract_role', 'hot_wallet', 'Operational hot wallet', 1),
    ('taxonomy_v1', 'contract_role', 'deposit_wallet', 'Exchange or service deposit wallet', 1),
    ('taxonomy_v1', 'contract_role', 'deployer', 'Contract deployment address', 1),
    ('taxonomy_v1', 'contract_role', 'unknown', 'No supported address role is established', 1)
)
WHERE (taxonomy_version, label_dimension, label_value) NOT IN
(
    SELECT taxonomy_version, label_dimension, label_value
    FROM chainlens.label_taxonomy
);

-- Run state changes append a higher version. Output rows are immutable and scoped
-- by label_run_id; a failed execution is never selected by current read views.
CREATE TABLE IF NOT EXISTS chainlens.label_runs
(
    run_id UUID,
    chain_id UInt64,
    cluster_run_id Nullable(UUID),
    taxonomy_version String,
    seed_dataset_version String,
    status Enum8('pending' = 1, 'running' = 2, 'completed' = 3, 'failed' = 4),
    started_at DateTime64(3, 'UTC'),
    completed_at Nullable(DateTime64(3, 'UTC')),
    seed_count UInt64,
    direct_label_count UInt64,
    propagated_label_count UInt64,
    entity_count UInt64,
    error Nullable(String),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id);

CREATE TABLE IF NOT EXISTS chainlens.entities
(
    chain_id UInt64,
    entity_id String,
    display_name Nullable(String),
    entity_category LowCardinality(String),
    created_from LowCardinality(String),
    cluster_id Nullable(String),
    cluster_run_id Nullable(UUID),
    label_run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, label_run_id, entity_id);

CREATE TABLE IF NOT EXISTS chainlens.entity_members
(
    chain_id UInt64,
    entity_id String,
    wallet_address String,
    membership_method LowCardinality(String),
    membership_confidence Float32,
    cluster_id Nullable(String),
    cluster_run_id Nullable(UUID),
    label_run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, label_run_id, entity_id, wallet_address, membership_method);

CREATE TABLE IF NOT EXISTS chainlens.label_assignments
(
    chain_id UInt64,
    subject_type Enum8('address' = 1, 'entity' = 2),
    subject_id String,
    entity_id Nullable(String),
    label_dimension LowCardinality(String),
    label_value String,
    confidence Float32,
    assignment_method LowCardinality(String),
    source_type LowCardinality(String),
    source_reference String,
    label_run_id UUID,
    taxonomy_version String,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    chain_id, label_run_id, subject_type, subject_id,
    label_dimension, label_value, assignment_method
);

CREATE TABLE IF NOT EXISTS chainlens.label_evidence
(
    chain_id UInt64,
    subject_type Enum8('address' = 1, 'entity' = 2),
    subject_id String,
    label_dimension LowCardinality(String),
    label_value String,
    evidence_type LowCardinality(String),
    evidence_value String,
    weight Float32,
    confidence_contribution Float32,
    source_type LowCardinality(String),
    source_reference String,
    label_run_id UUID,
    derived_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (
    chain_id, label_run_id, subject_type, subject_id,
    label_dimension, label_value, evidence_type, evidence_value
);

CREATE TABLE IF NOT EXISTS chainlens.label_conflicts
(
    chain_id UInt64,
    cluster_id String,
    cluster_run_id UUID,
    conflict_type LowCardinality(String),
    details String,
    label_run_id UUID,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, label_run_id, cluster_id, conflict_type);

-- Resolve each execution's logical state first, then select the newest completed
-- run per fully reproducible input scope. UUID breaks equal-timestamp ties.
CREATE VIEW IF NOT EXISTS chainlens.latest_label_runs AS
SELECT
    chain_id,
    cluster_run_id,
    taxonomy_version,
    seed_dataset_version,
    tupleElement(latest, 1) AS run_id,
    tupleElement(latest, 2) AS started_at,
    tupleElement(latest, 3) AS completed_at,
    tupleElement(latest, 4) AS seed_count,
    tupleElement(latest, 5) AS direct_label_count,
    tupleElement(latest, 6) AS propagated_label_count,
    tupleElement(latest, 7) AS entity_count,
    tupleElement(latest, 8) AS updated_at,
    tupleElement(latest, 9) AS version
FROM
(
    SELECT
        chain_id,
        cluster_run_id,
        taxonomy_version,
        seed_dataset_version,
        argMax(
            tuple(
                run_id, started_at, completed_at, seed_count, direct_label_count,
                propagated_label_count, entity_count, updated_at, version
            ),
            tuple(assumeNotNull(completed_at), toString(run_id))
        ) AS latest
    FROM
    (
        SELECT
            run_id,
            tupleElement(state, 1) AS chain_id,
            tupleElement(state, 2) AS cluster_run_id,
            tupleElement(state, 3) AS taxonomy_version,
            tupleElement(state, 4) AS seed_dataset_version,
            tupleElement(state, 5) AS status,
            tupleElement(state, 6) AS started_at,
            tupleElement(state, 7) AS completed_at,
            tupleElement(state, 8) AS seed_count,
            tupleElement(state, 9) AS direct_label_count,
            tupleElement(state, 10) AS propagated_label_count,
            tupleElement(state, 11) AS entity_count,
            tupleElement(state, 12) AS updated_at,
            max_version AS version
        FROM
        (
            SELECT
                run_id,
                argMax(
                    tuple(
                        chain_id, cluster_run_id, taxonomy_version,
                        seed_dataset_version, status, started_at, completed_at,
                        seed_count, direct_label_count, propagated_label_count,
                        entity_count, updated_at
                    ),
                    version
                ) AS state,
                max(version) AS max_version
            FROM chainlens.label_runs
            GROUP BY run_id
        )
    )
    WHERE status = 'completed'
    GROUP BY chain_id, cluster_run_id, taxonomy_version, seed_dataset_version
);

CREATE VIEW IF NOT EXISTS chainlens.current_entities AS
SELECT values.*
FROM chainlens.entities AS values
INNER JOIN chainlens.latest_label_runs AS latest
    ON values.chain_id = latest.chain_id AND values.label_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_entity_members AS
SELECT values.*
FROM chainlens.entity_members AS values
INNER JOIN chainlens.latest_label_runs AS latest
    ON values.chain_id = latest.chain_id AND values.label_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_label_assignments AS
SELECT values.*
FROM chainlens.label_assignments AS values
INNER JOIN chainlens.latest_label_runs AS latest
    ON values.chain_id = latest.chain_id AND values.label_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_label_evidence AS
SELECT values.*
FROM chainlens.label_evidence AS values
INNER JOIN chainlens.latest_label_runs AS latest
    ON values.chain_id = latest.chain_id AND values.label_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_label_conflicts AS
SELECT values.*
FROM chainlens.label_conflicts AS values
INNER JOIN chainlens.latest_label_runs AS latest
    ON values.chain_id = latest.chain_id AND values.label_run_id = latest.run_id;
