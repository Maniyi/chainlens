# ChainLens

ChainLens is a portfolio project for building a small, production-style blockchain
wallet intelligence platform. This first milestone provides only the Python project
foundation, ClickHouse service, initial ingestion tables, and connectivity checks.

## Architecture scope

Milestone 1 contains:

- a single local ClickHouse server managed by Docker Compose;
- fork-aware tables for observed EVM blocks, transactions, and token transfers;
- canonical block state, its audit history, and reorganization event metadata;
- versioned pipeline run and block-hash-aware checkpoint tables;
- Python configuration and a small ClickHouse client factory;
- a command-line health check and an integration connectivity test.

Later ingestion, graph, clustering, labeling, evaluation, and GraphQL capabilities
are intentionally outside this milestone.

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

The defaults work with the Compose service, so creating `.env` is optional.

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

The RPC ingestion and reorg-detection logic is not implemented in Milestone 1.

## Run the health check

```bash
python -m chainlens.health
```

## Run tests

With ClickHouse running:

```bash
pytest
```
