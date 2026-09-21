# ChainLens

ChainLens is a small, production-style blockchain data project. Milestone 2 adds
bounded Base/EVM ingestion to the fork-aware ClickHouse foundation from Milestone 1.

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
- a command-line health check plus deterministic unit and integration tests.

Graph construction, clustering, labeling, GraphQL, streaming, and frontend work are
intentionally outside this milestone.

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
```

## Start ClickHouse

```bash
docker compose up -d
docker compose ps
```

## Apply the schema

Run both migrations in order through the client bundled in the ClickHouse
container. If `001_initial.sql` was applied previously, run only the second command.

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
