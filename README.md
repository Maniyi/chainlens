# ChainLens

ChainLens is a small, production-style blockchain data project. Milestone 5 adds a
scoped evaluation and deterministic drift-monitoring layer to the fork-aware
Base/EVM ingestion, wallet-intelligence, and entity-labeling layers.

## Architecture scope

The implemented scope contains:

- a single local ClickHouse server managed by Docker Compose;
- fork-aware tables for observed EVM blocks, transactions, and token transfers;
- canonical block state, its audit history, and reorganization event metadata;
- versioned pipeline run and block-hash-aware checkpoint tables;
- Python configuration and a small ClickHouse client factory;
- direct Ethereum JSON-RPC ingestion through `httpx`;
- ERC-20 `Transfer(address,address,uint256)` decoding from transaction receipts;
- resumable bounded backfills, protocol finality tracking, and ancestry-based reorg
  recovery;
- canonical native/ERC-20 interaction edges, candidate first-funder provenance,
  lightweight wallet features, and explainable shared-funder entity edges;
- deterministic connected components and candidate wallet clusters;
- curated entity/address labels, explicit memberships, evidence, confidence, and
  conservative cluster-based category propagation;
- immutable label snapshots plus latest-completed read models;
- separate direct/inferred label evaluation, pairwise clustering evaluation,
  coverage statistics, operational drift metrics, and rule-based alerts;
- immutable evaluation history plus latest-completed compatible-scope views;
- a command-line health check plus deterministic unit and integration tests.

MEV/searcher/sniper/bot classifiers, deposit-address and common-spending heuristics,
ML, GraphQL, streaming, alert delivery, and frontend work remain outside this
milestone.

## Local setup

Requirements: Python 3.11 and Docker with Docker Compose.

Create and activate a virtual environment, install the project, and optionally copy
the environment template:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
```

The ClickHouse defaults work with Compose. RPC ingestion additionally requires a
Base endpoint in `.env` (credentials belong only in that ignored local file):

```dotenv
BASE_RPC_URL=https://your-base-rpc.example
RPC_TIMEOUT_SECONDS=20
RPC_BATCH_SIZE=20
REORG_MAX_DEPTH=128
CLUSTER_SHARED_FUNDER_WINDOW_SECONDS=3600
CLUSTER_MAX_FUNDER_FANOUT=20
LABEL_PROPAGATION_FACTOR=0.95
DRIFT_MAX_LARGEST_CLUSTER_RELATIVE_INCREASE=1.0
DRIFT_CLUSTER_COUNT_DROP_THRESHOLD=0.5
DRIFT_PROPAGATED_LABEL_SPIKE_THRESHOLD=1.0
DRIFT_CONFLICT_COUNT_THRESHOLD=1
DRIFT_SEED_COVERAGE_DROP_THRESHOLD=0.2
```

## Start ClickHouse

```bash
docker compose up -d
docker compose ps
```

## Apply the schema

Run the migrations in order through the client bundled in the ClickHouse container.
Apply only migrations newer than the current database schema.

```bash
docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/001_initial.sql

docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/002_reorg_aware_ingestion.sql

docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/003_wallet_clustering.sql

docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/004_clustering_snapshot_history.sql

docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/005_entity_labels.sql

docker compose exec -T clickhouse clickhouse-client \
  --user chainlens \
  --password chainlens \
  --database chainlens \
  --multiquery < warehouse/migrations/006_evaluation_and_drift.sql
```

Migration `002` drops and recreates the affected ingestion and pipeline tables. This
is intentionally destructive for the current empty local-development database, so
stop writers before applying it. Its guards make it safe to rerun after interruption.

## Reorg and canonicality model

Raw blockchain facts are fork-aware. A block hash ties each transaction and token
transfer to the exact observed block, and facts from orphaned blocks remain stored.
`canonical_blocks` is a versioned current-state table selecting the canonical block
for each chain height, while `canonical_blocks_history` preserves the append-only
decision audit trail. `reorg_events` stores summaries of detected reorganizations.

Pipeline checkpoints store both block number and block hash so future resume logic
can detect a changed canonical block. The `unsafe`, `safe`, and `finalized` values
represent protocol security state. Normal ClickHouse views expose current canonical
blocks, transactions, and transfers using `argMax` rather than `FINAL`.

```text
Observed:             Reorg:
100 -> A              100 -> A
101 -> B              101 -> X
102 -> C              102 -> Y

Stored raw facts retain: B, C, X, Y

Current canonical state:
100 -> A
101 -> X
102 -> Y
```

## Run a bounded backfill

The endpoint chain ID is checked before any blockchain facts are ingested. Base
mainnet is chain ID `8453`. Both bounds are required; the command never defaults to
genesis or starts a live follower.

```bash
python -m chainlens.ingestion.runner backfill \
  --chain-id 8453 \
  --from-block 20000000 \
  --to-block 20000019
```

For each block the write order is raw block, raw transactions, token transfers,
canonical current state, canonical history, then checkpoint. ClickHouse does not
offer a multi-table transaction for this workflow, so the checkpoint is deliberately
last: a crash before it can safely replay the block. ReplacingMergeTree may contain
temporary physical duplicates, but logical identities and all current-state reads
remain idempotent without relying on `FINAL`.

## Reorg recovery and finality

Each new block's parent hash is compared with the preceding stored canonical hash.
A conflicting block at an already canonical height is also a reorg signal. Recovery
walks the RPC branch backward and compares hashes at equal heights using the
`current_canonical_blocks` (`argMax`) view. It stops at a common ancestor or fails
once `REORG_MAX_DEPTH` is exceeded.

Recovery retains every orphaned row in `raw_blocks`, `raw_transactions`, and
`token_transfers`. It appends non-canonical audit entries for the old branch, ingests
and selects the replacement branch, writes one `reorg_events` summary, and repairs
the hash-aware checkpoint. Resume always re-fetches the checkpoint height and
recovers first if its hash changed; it never trusts height alone.

Security levels come from Base/OP Stack's `latest`, `safe`, and `finalized` RPC tags,
not a confirmation count. A block at or below the finalized head is `finalized`; a
block above finalized and at or below safe is `safe`; newer blocks are `unsafe`.
Security upgrades append newer canonical versions and history rather than rewriting
raw facts.

## Wallet clustering model

An interaction edge is not an ownership edge. `wallet_transfer_edges` records that
two wallets exchanged native value or an ERC-20 token; it does **not** assert that
they belong to one entity. Only `entity_resolution_edges`, produced by explicit
high-confidence heuristics, are supplied to connected components.

The bounded pipeline reads only `canonical_transactions` and
`canonical_token_transfers`, then produces:

- `wallet_transfer_edges`: native and ERC-20 wallet interactions with stable SHA-256
  edge IDs;
- `wallet_funding_edges`: the earliest successful, non-zero inbound native transfer
  per wallet in the analyzed range, labeled `first_native_funder`;
- `wallet_features`: first/last activity, directional counts and native amounts,
  distinct counterparties, and first-funder fields;
- `entity_resolution_edges`: canonicalized wallet pairs supported by the
  `shared_funder` heuristic;
- `cluster_evidence`: the same funder, funding-time delta, and funder fanout behind
  every resolution edge;
- `wallet_clusters`: non-singleton connected components over entity-resolution edges
  only.

For example, if `F -> A`, `F -> B`, and `F -> C` are each a wallet's first native
funding event, `F` is below the configured fanout limit, and the events fall within
the configured time window, the heuristic can emit `A-B`, `A-C`, and `B-C`. Those
resolution edges produce candidate cluster `{A, B, C}`. Shared funding is a
heuristic, not proof of common ownership.

The score is transparent: `0.60` for a shared first funder, up to `0.25` for time
proximity (`0.25 * (1 - delta/window)`), and up to `0.15` for low funder fanout,
clamped to `[0, 1]`. Every contribution is stored separately. Cluster confidence is
the minimum resolution-edge score inside the component; it is a deterministic
heuristic score, not statistically calibrated confidence.

Pair ordering, edge IDs, transaction tie-breaking, and cluster IDs are deterministic.
Cluster IDs hash the chain ID and sorted member addresses. Execution UUIDs identify
runs, not logical clusters. Every completed execution is an immutable snapshot scoped
by chain ID, block range, and heuristic version. Repeating the same scope creates a
new run without deleting any earlier completed output. Failed runs can be restarted
with `--resume-run-id`; cleanup is limited to incomplete rows for that same run ID,
and current state remains append-versioned in `cluster_runs`.

Historical tables retain every completed snapshot for reproducibility, auditability,
and point-in-time inspection. `latest_cluster_runs` resolves the latest completed run
for each scope using logical `argMax(..., version)` run state, and the `current_*`
views expose that run as a replaceable read model without copying or deleting data.
Failed and still-running executions never become current. Recomputing a range after a
reorg therefore preserves both the earlier and recomputed snapshots for comparison.
The runner also compares every canonical block hash/version before and after its
fact reads and fails cleanly if canonical state changes during execution.

Wallets without an entity-resolution edge remain unclustered. ChainLens does not
write them as singleton entities.

## Run bounded wallet clustering

Both block bounds are required; the command never defaults to the whole warehouse:

```bash
python -m chainlens.clustering.runner run \
  --chain-id 8453 \
  --from-block 20000000 \
  --to-block 20000019
```

To restart a failed execution with its original run identity and matching bounds:

```bash
python -m chainlens.clustering.runner run \
  --chain-id 8453 \
  --from-block 20000000 \
  --to-block 20000019 \
  --resume-run-id <failed-run-uuid>
```

## Inspect wallet intelligence

Use the completed `run_id` printed by the bounded command to inspect an exact
historical snapshot. For the latest completed snapshot of each scope, query the
corresponding `current_*` view.

Historical and current cluster access:

```sql
-- Reproduce one older execution exactly.
SELECT * FROM chainlens.wallet_clusters
WHERE cluster_run_id = '<historical-run-id>';

-- Read the latest completed execution for this scope.
SELECT clusters.*
FROM chainlens.current_wallet_clusters AS clusters
INNER JOIN chainlens.latest_cluster_runs AS latest
    ON clusters.cluster_run_id = latest.run_id
WHERE latest.chain_id = 8453
  AND latest.start_block = 20000000
  AND latest.end_block = 20000019
  AND latest.heuristic_version = 'shared_funder_v1';
```

Top funders by funded-wallet count:

```sql
SELECT funder_address, uniqExact(funded_address) AS funded_wallets
FROM chainlens.wallet_funding_edges
WHERE chain_id = 8453 AND run_id = '<run-id>'
GROUP BY funder_address
ORDER BY funded_wallets DESC;
```

Largest candidate clusters and their members:

```sql
SELECT cluster_id, any(cluster_size) AS size, any(confidence) AS confidence
FROM chainlens.wallet_clusters
WHERE chain_id = 8453 AND cluster_run_id = '<run-id>'
GROUP BY cluster_id
ORDER BY size DESC;

SELECT wallet_address, confidence
FROM chainlens.wallet_clusters
WHERE chain_id = 8453 AND cluster_run_id = '<run-id>'
  AND cluster_id = '<cluster-id>'
ORDER BY wallet_address;
```

Resolution edges for a wallet and evidence explaining a pair:

```sql
SELECT wallet_a, wallet_b, heuristic, score
FROM chainlens.entity_resolution_edges
WHERE chain_id = 8453 AND run_id = '<run-id>'
  AND (wallet_a = '<wallet>' OR wallet_b = '<wallet>');

SELECT evidence_type, evidence_value, weight, score_contribution
FROM chainlens.cluster_evidence
WHERE chain_id = 8453 AND run_id = '<run-id>'
  AND wallet_a = '<lexicographically-smaller-wallet>'
  AND wallet_b = '<lexicographically-larger-wallet>'
ORDER BY evidence_type;
```

Wallet feature inspection:

```sql
SELECT * FROM chainlens.wallet_features
WHERE chain_id = 8453 AND run_id = '<run-id>'
  AND wallet_address = '<wallet>';
```

## Entity and label model

Milestone 4 never stores an unexplained flat label. A label assignment identifies
its address or entity subject, dimension, value, confidence, assignment method,
source type/reference, taxonomy version, label run, and timestamp. Separate evidence
rows retain the human-readable reason and the inputs to propagated confidence.

The `taxonomy_v1` taxonomy is intentionally small. Entity categories are `dex`,
`exchange`, `bridge`, `protocol`, `token_issuer`, and `unknown`. Address-specific
contract roles are `router`, `factory`, `pool_manager`, `position_manager`,
`token_contract`, `protocol_contract`, `hot_wallet`, `deposit_wallet`, `deployer`,
and `unknown`. An address need not have a role, and a role is never copied merely
because two addresses are members of the same entity.

Curated addresses with the same normalized `entity_name` share a stable entity ID:
`seed_` plus SHA-256 over a length-prefixed encoding of `(chain_id,
casefolded-and-whitespace-normalized entity_name)`. Cluster candidate entities use
`cluster_` plus SHA-256 over `(chain_id, cluster_id)`. Execution UUIDs identify label
runs only; they are not logical entity identities. A curated address that is also in
a selected candidate cluster can therefore have both a `direct_seed` membership in
its named curated entity and a `cluster_membership` in the candidate cluster entity.

The curated Base CSV currently contains 17 lowercase addresses covering Uniswap,
Aerodrome, Base WETH, Circle USDC, and Permit2. The loader validates the chain,
address, required name, taxonomy values, provenance, and confidence range. Exact
duplicate rows collapse; conflicting duplicate address rows fail. A deterministic
`sha256:` digest over normalized, sorted semantic rows is stored as
`seed_dataset_version`, so row order and CSV formatting do not identify the dataset.

Direct and propagated assignments are deliberately different:

```text
DIRECT
SwapRouter02 address
  -> curated Uniswap entity
  -> entity_category=dex, contract_role=router
  -> official source reference, confidence=1.0

PROPAGATED
directly labeled address that is an actual selected-cluster member
  -> candidate cluster entity
  -> entity_category only for that entity and its unlabeled members
  -> confidence = seed confidence * member confidence * propagation factor

FORBIDDEN
wallet transfers to or calls a Uniswap router
  -/-> Uniswap, dex, or router label
```

The default propagation factor is `0.95`; the result is clamped to `[0,1]`. For
example, `1.0 * 0.9569 * 0.95 = 0.909055`. This is a deterministic heuristic
confidence, not a statistically calibrated probability. Evidence rows preserve the
labeled source member, membership confidence, propagation factor, and original
source. Direct confidence is not reduced when clustering also exists.

Only membership from the selected `wallet_clusters` snapshot can trigger
propagation. `wallet_transfer_edges` are never consulted. `entity_category` may
propagate; `contract_role` never does. If one cluster contains direct seeds for
different identities, both direct address labels remain, a
`conflicting_direct_seed_identities` row records the ambiguity, the candidate entity
stays unknown, and nothing propagates to unlabeled members.

`entities`, `entity_members`, `label_assignments`, `label_evidence`, and
`label_conflicts` retain immutable rows for every execution. `label_runs` appends
higher-version state transitions. `latest_label_runs` resolves the newest completed
execution per `(chain_id, cluster_run_id, taxonomy_version, seed_dataset_version)`
without `FINAL`; failed or running executions never become current. The corresponding
`current_*` views join only those completed snapshots. Physical historical tables
remain directly queryable by `label_run_id`.

## Run entity labeling

Select a completed cluster run explicitly for reproducibility:

```bash
python -m chainlens.labels.runner run \
  --chain-id 8453 \
  --cluster-run-id <completed-cluster-run-uuid> \
  --seed-file data/ground_truth/base_known_addresses.csv \
  --taxonomy-version taxonomy_v1
```

Omitting `--cluster-run-id` is explicit direct-only mode; the runner does not guess
among clustering scopes. Curated contracts are still represented and labeled when
they do not occur in wallet clusters. A run can truthfully report zero propagated
labels.

## Inspect labels

Direct labels and provenance for one address:

```sql
SELECT label_dimension, label_value, confidence, assignment_method,
       source_type, source_reference, entity_id, label_run_id
FROM chainlens.current_label_assignments
WHERE chain_id = 8453
  AND subject_type = 'address'
  AND subject_id = '<address>';
```

Entity members and full evidence:

```sql
SELECT wallet_address, membership_method, membership_confidence,
       cluster_id, cluster_run_id
FROM chainlens.current_entity_members
WHERE chain_id = 8453 AND entity_id = '<entity-id>'
ORDER BY wallet_address;

SELECT evidence_type, evidence_value, weight, confidence_contribution,
       source_type, source_reference
FROM chainlens.current_label_evidence
WHERE chain_id = 8453 AND subject_id = '<address-or-entity-id>'
ORDER BY label_dimension, evidence_type;
```

Current and historical labels:

```sql
SELECT * FROM chainlens.current_label_assignments
WHERE chain_id = 8453;

SELECT * FROM chainlens.label_assignments
WHERE label_run_id = '<historical-label-run-id>';
```

Conflicts, named-entity addresses, and entities by category:

```sql
SELECT * FROM chainlens.current_label_conflicts
WHERE chain_id = 8453;

SELECT members.wallet_address, members.membership_method
FROM chainlens.current_entity_members AS members
INNER JOIN chainlens.current_entities AS entities
  ON members.chain_id = entities.chain_id
 AND members.label_run_id = entities.label_run_id
 AND members.entity_id = entities.entity_id
WHERE entities.chain_id = 8453 AND entities.display_name = 'Uniswap'
ORDER BY members.wallet_address;

SELECT entity_id, display_name, created_from
FROM chainlens.current_entities
WHERE chain_id = 8453 AND entity_category = 'dex'
ORDER BY display_name, entity_id;
```

## Evaluation and drift monitoring

Evaluation is split into three targets because they answer different questions:

- **Label evaluation** checks address-level `entity_category` and `contract_role`
  assignments against curated truth. `direct_seed` and `cluster_propagation` are
  always separate. Direct results are data-loading correctness checks, not evidence
  that inference works. Predictions on subjects with no curated truth are counted as
  predictions but are not called false positives because their correctness is
  unknown.
- **Clustering evaluation** uses pairwise precision, recall, and F1 among curated
  addresses that were actually seen in the selected clustering run's wallet scope.
  A true-positive pair has the same curated entity and predicted cluster; a
  false-positive pair crosses curated entities in one predicted cluster; a
  false-negative pair belongs to one curated entity but is split or unclustered.
  Unknown-identity pairs and true negatives are excluded. Pairwise metrics directly
  measure erroneous merges and splits and do not reward the many unrelated pairs.
- **Operational drift** describes snapshot size, cluster-size distribution,
  confidence, label counts/distributions, conflicts, and seed overlap. Coverage says
  how much curated truth intersects the selected data; it is not accuracy.

Precision is the fraction of evaluable predictions that are correct. Recall is the
fraction of curated positives recovered. F1 is their harmonic mean. A denominator
of zero produces SQL `NULL`, not a manufactured zero. For example, zero propagated
labels gives undefined precision, while recall is zero when curated positives exist.
Clustering precision/recall/F1 are all undefined when no curated address pairs are in
scope.

The deterministic ground-truth version is the normalized semantic `sha256:` digest
already used for seed datasets, not a filename. `evaluation_v1` explicitly versions
metric semantics. Evaluation runs append pending/running/completed/failed state,
and all result rows remain immutable and scoped by `evaluation_run_id`.
`latest_evaluation_runs` resolves the newest completed run for the exact compatible
chain, block range, heuristic version, taxonomy version, ground-truth version, and
evaluation version. Failed runs never replace current results. The four
`current_*` evaluation views join to that completed snapshot; historical tables stay
directly queryable.

Coverage metrics include seed count, seeds seen in the selected wallet scope, seeds
present in candidate clusters, directly labeled curated addresses, and curated
addresses with inferred labels. Drift metrics also record wallet/edge/cluster counts,
largest/average/median/p95 cluster size, average cluster confidence, entity/direct/
propagated/conflict counts, average propagated confidence, and label counts by
dimension/value.

Alerting is deterministic and deliberately small: largest-cluster expansion,
cluster-count collapse while wallet count is stable, propagation spikes, conflict
increases, and seed seen/cluster-overlap drops. Environment settings control the
guardrails. They are operational thresholds, not statistical hypothesis tests or
claims of significance. No external notification delivery is implemented.

**Current evaluation results are limited by the small curated ground-truth set and
should not be interpreted as whole-chain accuracy.** The current seeds mostly name
protocol contracts rather than ordinary wallets. Zero cluster overlap therefore
means there is no pairwise support, not that clustering is perfect or broken. The
system does not estimate whole-chain recall or production-grade statistical
confidence.

Run an evaluation only with explicit, completed snapshot IDs:

```bash
python -m chainlens.evaluation.runner run \
  --chain-id 8453 \
  --cluster-run-id <completed-cluster-run-uuid> \
  --label-run-id <completed-label-run-uuid> \
  --ground-truth data/ground_truth/base_known_addresses.csv
```

The selected label run must reference the selected cluster run. Repeating identical
inputs creates another historical evaluation execution with deterministic logical
metrics; it does not overwrite the earlier snapshot.

Latest evaluation execution:

```sql
SELECT * FROM chainlens.latest_evaluation_runs
WHERE chain_id = 8453
ORDER BY completed_at DESC;
```

Label precision, recall, F1, support, predictions, and coverage:

```sql
SELECT label_dimension, label_value, assignment_method,
       tp, fp, fn, precision, recall, f1, support, predicted_count, coverage
FROM chainlens.current_label_evaluations
WHERE chain_id = 8453
ORDER BY assignment_method, label_dimension, label_value;
```

Pairwise clustering support and metrics:

```sql
SELECT metric_scope, ground_truth_address_count, evaluated_address_count,
       tp_pairs, fp_pairs, fn_pairs,
       pairwise_precision, pairwise_recall, pairwise_f1
FROM chainlens.current_clustering_evaluations
WHERE chain_id = 8453;
```

Coverage, largest-cluster drift, and label-distribution drift:

```sql
SELECT metric_name, value, previous_value, absolute_change, relative_change
FROM chainlens.current_drift_metrics
WHERE chain_id = 8453 AND metric_group = 'coverage'
ORDER BY metric_name;

SELECT value, previous_value, relative_change
FROM chainlens.current_drift_metrics
WHERE chain_id = 8453 AND metric_name = 'largest_cluster_size';

SELECT scope_key, value, previous_value, relative_change
FROM chainlens.current_drift_metrics
WHERE chain_id = 8453 AND metric_name = 'label_count'
ORDER BY scope_key;
```

Alerts and one exact historical evaluation:

```sql
SELECT severity, metric_name, scope_key, current_value, previous_value,
       threshold_type, threshold_value, message
FROM chainlens.current_drift_alerts
WHERE chain_id = 8453
ORDER BY severity DESC, metric_name;

SELECT * FROM chainlens.label_evaluations
WHERE evaluation_run_id = '<evaluation-run-id>';
SELECT * FROM chainlens.clustering_evaluations
WHERE evaluation_run_id = '<evaluation-run-id>';
SELECT * FROM chainlens.drift_metrics
WHERE evaluation_run_id = '<evaluation-run-id>';
SELECT * FROM chainlens.drift_alerts
WHERE evaluation_run_id = '<evaluation-run-id>';
```

## Inspect ingestion state

Current canonical blocks:

```sql
SELECT * FROM chainlens.current_canonical_blocks
WHERE chain_id = 8453 ORDER BY block_number;
```

Canonical transactions:

```sql
SELECT block_number, tx_hash, status
FROM chainlens.canonical_transactions
WHERE chain_id = 8453 ORDER BY block_number, tx_index;
```

All observed forks at one height (including orphaned observations):

```sql
SELECT block_number, block_hash, parent_hash, max(ingested_at)
FROM chainlens.raw_blocks
WHERE chain_id = 8453 AND block_number = 20000000
GROUP BY block_number, block_hash, parent_hash;
```

Reorg events:

```sql
SELECT * FROM chainlens.reorg_events
WHERE chain_id = 8453 ORDER BY detected_at DESC;
```

Current checkpoint without assuming background merges:

```sql
SELECT argMax(
  tuple(last_processed_block, last_processed_block_hash, updated_at), version
) AS checkpoint
FROM chainlens.pipeline_checkpoints
WHERE job_name = 'block_ingestion' AND chain_id = 8453;
```

## Run the health check

```bash
python -m chainlens.health
```

## Run tests

With ClickHouse running:

```bash
pytest
```

The deterministic tests use mocked RPC behavior. A real endpoint is needed only for
manual backfill validation.
