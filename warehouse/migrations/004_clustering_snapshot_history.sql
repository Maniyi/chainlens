-- Milestone 3 correction: retain immutable run snapshots and expose the latest
-- completed execution per (chain, range, heuristic version) as replaceable views.

CREATE VIEW IF NOT EXISTS chainlens.latest_cluster_runs AS
SELECT
    chain_id,
    start_block,
    end_block,
    heuristic_version,
    tupleElement(latest, 1) AS run_id,
    tupleElement(latest, 2) AS started_at,
    tupleElement(latest, 3) AS completed_at,
    tupleElement(latest, 4) AS wallet_count,
    tupleElement(latest, 5) AS resolution_edge_count,
    tupleElement(latest, 6) AS cluster_count,
    tupleElement(latest, 7) AS updated_at,
    tupleElement(latest, 8) AS version
FROM
(
    SELECT
        chain_id,
        start_block,
        end_block,
        heuristic_version,
        argMax(
            tuple(
                run_id,
                started_at,
                completed_at,
                wallet_count,
                resolution_edge_count,
                cluster_count,
                updated_at,
                version
            ),
            tuple(assumeNotNull(completed_at), toString(run_id))
        ) AS latest
    FROM
    (
        -- Resolve pending/running/completed/failed versions for each execution
        -- without relying on background ReplacingMergeTree merges or FINAL.
        SELECT
            run_id,
            tupleElement(state, 1) AS chain_id,
            tupleElement(state, 2) AS start_block,
            tupleElement(state, 3) AS end_block,
            tupleElement(state, 4) AS heuristic_version,
            tupleElement(state, 5) AS status,
            tupleElement(state, 6) AS started_at,
            tupleElement(state, 7) AS completed_at,
            tupleElement(state, 8) AS wallet_count,
            tupleElement(state, 9) AS resolution_edge_count,
            tupleElement(state, 10) AS cluster_count,
            tupleElement(state, 11) AS updated_at,
            max_version AS version
        FROM
        (
            SELECT
                run_id,
                argMax(
                    tuple(
                        chain_id,
                        start_block,
                        end_block,
                        heuristic_version,
                        status,
                        started_at,
                        completed_at,
                        wallet_count,
                        resolution_edge_count,
                        cluster_count,
                        updated_at
                    ),
                    version
                ) AS state,
                max(version) AS max_version
            FROM chainlens.cluster_runs
            GROUP BY run_id
        )
    )
    WHERE status = 'completed'
    GROUP BY chain_id, start_block, end_block, heuristic_version
);

CREATE VIEW IF NOT EXISTS chainlens.current_wallet_clusters AS
SELECT clusters.*
FROM chainlens.wallet_clusters AS clusters
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON clusters.chain_id = latest.chain_id
    AND clusters.cluster_run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_entity_resolution_edges AS
SELECT edges.*
FROM chainlens.entity_resolution_edges AS edges
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON edges.chain_id = latest.chain_id
    AND edges.run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_cluster_evidence AS
SELECT evidence.*
FROM chainlens.cluster_evidence AS evidence
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON evidence.chain_id = latest.chain_id
    AND evidence.run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_wallet_features AS
SELECT features.*
FROM chainlens.wallet_features AS features
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON features.chain_id = latest.chain_id
    AND features.run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_wallet_funding_edges AS
SELECT funding.*
FROM chainlens.wallet_funding_edges AS funding
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON funding.chain_id = latest.chain_id
    AND funding.run_id = latest.run_id;

CREATE VIEW IF NOT EXISTS chainlens.current_wallet_transfer_edges AS
SELECT transfers.*
FROM chainlens.wallet_transfer_edges AS transfers
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON transfers.chain_id = latest.chain_id
    AND transfers.run_id = latest.run_id;
