# ChainLens

ChainLens is a small, production-style blockchain data project. Milestone 3 adds
bounded wallet-intelligence derivation to the fork-aware Base/EVM ingestion layers.

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
- a command-line health check plus deterministic unit and integration tests.

Exchange/bridge/MEV/bot labels, deposit-address and common-spending heuristics, ML,
GraphQL, streaming, evaluation/drift monitoring, and frontend work remain outside
this milestone.

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
