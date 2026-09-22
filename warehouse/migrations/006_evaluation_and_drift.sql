-- Milestone 5: scoped evaluation history and deterministic drift monitoring.

CREATE TABLE IF NOT EXISTS chainlens.evaluation_runs
(
    run_id UUID,
    chain_id UInt64,
    cluster_run_id Nullable(UUID),
    label_run_id Nullable(UUID),
    start_block UInt64,
    end_block UInt64,
    heuristic_version String,
    taxonomy_version String,
    seed_dataset_version String,
    ground_truth_version String,
    evaluation_version String,
    status Enum8('pending' = 1, 'running' = 2, 'completed' = 3, 'failed' = 4),
    started_at DateTime64(3, 'UTC'),
    completed_at Nullable(DateTime64(3, 'UTC')),
    error Nullable(String),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (run_id);

CREATE TABLE IF NOT EXISTS chainlens.label_evaluations
(
    evaluation_run_id UUID,
    chain_id UInt64,
    label_dimension LowCardinality(String),
    label_value String,
    assignment_method LowCardinality(String),
    tp UInt64,
    fp UInt64,
    fn UInt64,
    precision Nullable(Float64),
    recall Nullable(Float64),
    f1 Nullable(Float64),
    support UInt64,
    predicted_count UInt64,
    coverage Float64,
    evaluated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, evaluation_run_id, label_dimension, assignment_method, label_value);

CREATE TABLE IF NOT EXISTS chainlens.clustering_evaluations
(
    evaluation_run_id UUID,
    chain_id UInt64,
    cluster_run_id UUID,
    metric_scope LowCardinality(String),
    tp_pairs UInt64,
    fp_pairs UInt64,
    fn_pairs UInt64,
    pairwise_precision Nullable(Float64),
    pairwise_recall Nullable(Float64),
    pairwise_f1 Nullable(Float64),
    ground_truth_entity_count UInt64,
    ground_truth_address_count UInt64,
    evaluated_address_count UInt64,
    predicted_cluster_count UInt64,
    evaluated_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, evaluation_run_id, metric_scope);

CREATE TABLE IF NOT EXISTS chainlens.drift_metrics
(
    evaluation_run_id UUID,
    chain_id UInt64,
    metric_group LowCardinality(String),
    metric_name String,
    scope_key String,
    value Float64,
    previous_value Nullable(Float64),
    absolute_change Nullable(Float64),
    relative_change Nullable(Float64),
    recorded_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, evaluation_run_id, metric_group, metric_name, scope_key);

CREATE TABLE IF NOT EXISTS chainlens.drift_alerts
(
    alert_id UUID,
    evaluation_run_id UUID,
    chain_id UInt64,
    metric_name String,
    scope_key String,
    severity Enum8('info' = 1, 'warning' = 2, 'critical' = 3),
    current_value Float64,
    previous_value Nullable(Float64),
    threshold_type LowCardinality(String),
    threshold_value Float64,
    message String,
    created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (chain_id, evaluation_run_id, severity, metric_name, alert_id);

-- Resolve each execution first, then select the newest completed exact-compatible
-- scope. Run IDs identify snapshots but are deliberately not compatibility keys.
CREATE VIEW IF NOT EXISTS chainlens.latest_evaluation_runs AS
SELECT
    chain_id, start_block, end_block, heuristic_version, taxonomy_version,
    ground_truth_version, evaluation_version,
    tupleElement(latest, 1) AS run_id,
    tupleElement(latest, 2) AS cluster_run_id,
    tupleElement(latest, 3) AS label_run_id,
    tupleElement(latest, 4) AS seed_dataset_version,
    tupleElement(latest, 5) AS started_at,
    tupleElement(latest, 6) AS completed_at,
    tupleElement(latest, 7) AS updated_at,
    tupleElement(latest, 8) AS version
FROM
(
    SELECT
        chain_id, start_block, end_block, heuristic_version, taxonomy_version,
        ground_truth_version, evaluation_version,
        argMax(
            tuple(run_id, cluster_run_id, label_run_id, seed_dataset_version,
                  started_at, completed_at, updated_at, version),
            tuple(assumeNotNull(completed_at), toString(run_id))
        ) AS latest
    FROM
    (
        SELECT
            run_id,
            tupleElement(state, 1) AS chain_id,
            tupleElement(state, 2) AS cluster_run_id,
            tupleElement(state, 3) AS label_run_id,
            tupleElement(state, 4) AS start_block,
            tupleElement(state, 5) AS end_block,
            tupleElement(state, 6) AS heuristic_version,
            tupleElement(state, 7) AS taxonomy_version,
            tupleElement(state, 8) AS seed_dataset_version,
            tupleElement(state, 9) AS ground_truth_version,
            tupleElement(state, 10) AS evaluation_version,
            tupleElement(state, 11) AS status,
            tupleElement(state, 12) AS started_at,
            tupleElement(state, 13) AS completed_at,
            tupleElement(state, 14) AS updated_at,
            max_version AS version
        FROM
        (
            SELECT
                run_id,
                argMax(tuple(
                    chain_id, cluster_run_id, label_run_id, start_block, end_block,
                    heuristic_version, taxonomy_version, seed_dataset_version,
                    ground_truth_version, evaluation_version, status, started_at,
                    completed_at, updated_at
                ), version) AS state,
                max(version) AS max_version
            FROM chainlens.evaluation_runs
            GROUP BY run_id
        )
    )
    WHERE status = 'completed'
    GROUP BY chain_id, start_block, end_block, heuristic_version, taxonomy_version,
             ground_truth_version, evaluation_version
);

CREATE VIEW IF NOT EXISTS chainlens.current_label_evaluations AS
SELECT values.* FROM chainlens.label_evaluations AS values
INNER JOIN chainlens.latest_evaluation_runs AS latest
    ON values.chain_id = latest.chain_id AND values.evaluation_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_clustering_evaluations AS
SELECT values.* FROM chainlens.clustering_evaluations AS values
INNER JOIN chainlens.latest_evaluation_runs AS latest
    ON values.chain_id = latest.chain_id AND values.evaluation_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_drift_metrics AS
SELECT values.* FROM chainlens.drift_metrics AS values
INNER JOIN chainlens.latest_evaluation_runs AS latest
    ON values.chain_id = latest.chain_id AND values.evaluation_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_drift_alerts AS
SELECT values.* FROM chainlens.drift_alerts AS values
INNER JOIN chainlens.latest_evaluation_runs AS latest
    ON values.chain_id = latest.chain_id AND values.evaluation_run_id = latest.run_id;
