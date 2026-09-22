"""ClickHouse schema and connectivity integration tests."""

from collections.abc import Iterator
import csv
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from clickhouse_connect.driver.client import Client

from chainlens.db import create_client
from chainlens.clustering.runner import ClusterRunner
from chainlens.clustering.store import ClickHouseClusteringStore
from chainlens.evaluation.logic import EVALUATION_VERSION
from chainlens.evaluation.models import DriftThresholds
from chainlens.evaluation.runner import EvaluationRunner
from chainlens.evaluation.store import ClickHouseEvaluationStore
from chainlens.evm.models import EvmBlock
from chainlens.ingestion.store import ClickHouseStore
from chainlens.labels.runner import LabelRunner
from chainlens.labels.seeds import REQUIRED_COLUMNS
from chainlens.labels.store import ClickHouseLabelStore


BASE_TABLES = {
    "raw_blocks",
    "raw_transactions",
    "token_transfers",
    "canonical_blocks",
    "canonical_blocks_history",
    "reorg_events",
    "pipeline_runs",
    "pipeline_checkpoints",
    "wallet_transfer_edges",
    "wallet_funding_edges",
    "wallet_features",
    "entity_resolution_edges",
    "cluster_evidence",
    "cluster_runs",
    "wallet_clusters",
    "label_runs",
    "entities",
    "entity_members",
    "label_assignments",
    "label_evidence",
    "label_conflicts",
    "evaluation_runs",
    "label_evaluations",
    "clustering_evaluations",
    "drift_metrics",
    "drift_alerts",
}

GLOBAL_TABLES = {"label_taxonomy"}

VIEWS = {
    "current_canonical_blocks",
    "canonical_transactions",
    "canonical_token_transfers",
    "latest_cluster_runs",
    "current_wallet_clusters",
    "current_entity_resolution_edges",
    "current_cluster_evidence",
    "current_wallet_features",
    "current_wallet_funding_edges",
    "current_wallet_transfer_edges",
    "latest_label_runs",
    "current_entities",
    "current_entity_members",
    "current_label_assignments",
    "current_label_evidence",
    "current_label_conflicts",
    "latest_evaluation_runs",
    "current_label_evaluations",
    "current_clustering_evaluations",
    "current_drift_metrics",
    "current_drift_alerts",
}


@pytest.fixture(scope="module")
def client() -> Iterator[Client]:
    """Share one ClickHouse connection across the integration module."""

    connection = create_client()
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def chain_id(client: Client) -> Iterator[int]:
    """Provide an isolated chain id and remove its rows after a test."""

    value = uuid4().int % (2**63)
    yield value

    for table in BASE_TABLES:
        client.command(
            f"ALTER TABLE chainlens.{table} DELETE WHERE chain_id = {value} "
            "SETTINGS mutations_sync = 1"
        )


def test_clickhouse_connectivity(client: Client) -> None:
    """The configured ClickHouse server should execute a basic query."""

    assert client.query("SELECT 1").result_rows == [(1,)]


def test_expected_tables_and_views_exist(client: Client) -> None:
    """The reorg-aware schema should expose all required objects."""

    names = {row[0] for row in client.query("SHOW TABLES FROM chainlens").result_rows}
    assert BASE_TABLES | GLOBAL_TABLES <= names
    assert VIEWS <= names


def test_fork_facts_and_canonical_views(client: Client, chain_id: int) -> None:
    """Raw forks coexist while canonical views select only the latest fork."""

    observed_at = datetime.now(UTC)
    raw_blocks = [
        (chain_id, 100, "AAA", "PARENT", observed_at, 1, observed_at),
        (chain_id, 100, "BBB", "PARENT", observed_at, 1, observed_at),
    ]
    client.insert(
        "chainlens.raw_blocks",
        raw_blocks,
        column_names=[
            "chain_id",
            "block_number",
            "block_hash",
            "parent_hash",
            "block_timestamp",
            "transaction_count",
            "ingested_at",
        ],
    )

    client.insert(
        "chainlens.canonical_blocks",
        [
            (chain_id, 100, "AAA", "PARENT", "unsafe", observed_at, 1),
            (chain_id, 100, "BBB", "PARENT", "safe", observed_at, 2),
        ],
        column_names=[
            "chain_id",
            "block_number",
            "block_hash",
            "parent_hash",
            "security_level",
            "observed_at",
            "version",
        ],
    )

    transaction_columns = [
        "chain_id",
        "block_number",
        "block_hash",
        "block_timestamp",
        "tx_hash",
        "tx_index",
        "from_address",
        "to_address",
        "value_wei",
        "gas",
        "gas_price",
        "input",
        "status",
        "ingested_at",
    ]
    client.insert(
        "chainlens.raw_transactions",
        [
            (
                chain_id,
                100,
                "AAA",
                observed_at,
                "tx_A",
                0,
                "from_A",
                "to_A",
                1,
                21_000,
                1,
                "0x",
                1,
                observed_at,
            ),
            (
                chain_id,
                100,
                "BBB",
                observed_at,
                "tx_B",
                0,
                "from_B",
                "to_B",
                2,
                21_000,
                1,
                "0x",
                1,
                observed_at,
            ),
        ],
        column_names=transaction_columns,
    )

    transfer_columns = [
        "chain_id",
        "block_number",
        "block_hash",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "token_address",
        "from_address",
        "to_address",
        "amount_raw",
        "ingested_at",
    ]
    client.insert(
        "chainlens.token_transfers",
        [
            (
                chain_id,
                100,
                "AAA",
                observed_at,
                "tx_A",
                0,
                "token",
                "from_A",
                "to_A",
                10,
                observed_at,
            ),
            (
                chain_id,
                100,
                "BBB",
                observed_at,
                "tx_B",
                0,
                "token",
                "from_B",
                "to_B",
                20,
                observed_at,
            ),
        ],
        column_names=transfer_columns,
    )

    current = client.query(
        "SELECT block_hash, security_level, version "
        "FROM chainlens.current_canonical_blocks "
        f"WHERE chain_id = {chain_id} AND block_number = 100"
    ).result_rows
    assert current == [("BBB", "safe", 2)]

    raw_hashes = client.query(
        "SELECT DISTINCT block_hash FROM chainlens.raw_blocks "
        f"WHERE chain_id = {chain_id} AND block_number = 100 "
        "ORDER BY block_hash"
    ).result_rows
    assert raw_hashes == [("AAA",), ("BBB",)]

    canonical_transactions = client.query(
        "SELECT tx_hash FROM chainlens.canonical_transactions "
        f"WHERE chain_id = {chain_id} ORDER BY tx_hash"
    ).result_rows
    assert canonical_transactions == [("tx_B",)]

    canonical_transfers = client.query(
        "SELECT tx_hash FROM chainlens.canonical_token_transfers "
        f"WHERE chain_id = {chain_id} ORDER BY tx_hash"
    ).result_rows
    assert canonical_transfers == [("tx_B",)]


def test_checkpoint_retains_height_and_hash(client: Client, chain_id: int) -> None:
    """Current checkpoint reads retain both reorg-sensitive coordinates."""

    updated_at = datetime.now(UTC)
    client.insert(
        "chainlens.pipeline_checkpoints",
        [
            ("block_ingestion", chain_id, 99, "AAA", updated_at, 1),
            ("block_ingestion", chain_id, 100, "BBB", updated_at, 2),
        ],
        column_names=[
            "job_name",
            "chain_id",
            "last_processed_block",
            "last_processed_block_hash",
            "updated_at",
            "version",
        ],
    )

    checkpoint = client.query(
        "SELECT tupleElement(state, 1), tupleElement(state, 2) "
        "FROM ("
        "  SELECT argMax("
        "    tuple(last_processed_block, last_processed_block_hash), version"
        "  ) AS state "
        "  FROM chainlens.pipeline_checkpoints "
        f"  WHERE job_name = 'block_ingestion' AND chain_id = {chain_id}"
        ")"
    ).result_rows
    assert checkpoint == [(100, "BBB")]


def test_milestone2_store_uses_current_state_and_idempotent_versions(
    client: Client, chain_id: int
) -> None:
    """The ingestion adapter round-trips through the existing versioned schema."""

    store = ClickHouseStore(client)
    now = datetime.now(UTC)
    block = EvmBlock(500, "0xabc", "0xparent", now)

    store.write_raw_facts(chain_id, block, [], [], now)
    assert store.set_canonical(chain_id, block, "safe", "observed", now)
    assert not store.set_canonical(chain_id, block, "safe", "observed", now)
    first_checkpoint = store.write_checkpoint(
        "block_ingestion", chain_id, block.number, block.block_hash
    )
    second_checkpoint = store.write_checkpoint(
        "block_ingestion", chain_id, block.number, block.block_hash
    )

    current = store.get_canonical_block(chain_id, block.number)
    checkpoint = store.get_checkpoint("block_ingestion", chain_id)
    history_count = client.query(
        "SELECT count() FROM chainlens.canonical_blocks_history "
        f"WHERE chain_id = {chain_id} AND block_number = {block.number}"
    ).first_row[0]
    assert current is not None
    assert (current.block_hash, current.security_level, current.version) == (
        block.block_hash,
        "safe",
        1,
    )
    assert checkpoint == first_checkpoint == second_checkpoint
    assert history_count == 1


def test_clustering_reads_only_canonical_facts(client: Client, chain_id: int) -> None:
    """An orphaned raw transaction must not enter the intelligence snapshot."""

    now = datetime.now(UTC)
    client.insert(
        "chainlens.canonical_blocks",
        [(chain_id, 700, "canonical", "parent", "safe", now, 1)],
        column_names=[
            "chain_id", "block_number", "block_hash", "parent_hash",
            "security_level", "observed_at", "version",
        ],
    )
    rows = []
    for block_hash, tx_hash in (("orphan", "orphan_tx"), ("canonical", "canonical_tx")):
        rows.append(
            (
                chain_id, 700, block_hash, now, tx_hash, 0,
                "0x" + "11" * 20, "0x" + "22" * 20,
                1, 21_000, 1, "0x", 1, now,
            )
        )
    client.insert(
        "chainlens.raw_transactions",
        rows,
        column_names=[
            "chain_id", "block_number", "block_hash", "block_timestamp", "tx_hash",
            "tx_index", "from_address", "to_address", "value_wei", "gas",
            "gas_price", "input", "status", "ingested_at",
        ],
    )

    transactions = ClickHouseClusteringStore(client).read_canonical_transactions(
        chain_id, 700, 700
    )
    assert [transaction.tx_hash for transaction in transactions] == ["canonical_tx"]


def test_clustering_snapshots_are_historical_and_current_views_select_latest(
    client: Client, chain_id: int
) -> None:
    """Completed snapshots remain queryable while failed runs never become current."""

    now = datetime.now(UTC)
    funder = "0x" + "11" * 20
    wallet_a = "0x" + "22" * 20
    wallet_b = "0x" + "33" * 20
    canonical_rows = [
        (chain_id, 800, "block_800", "parent", "safe", now, 1),
        (chain_id, 801, "block_801", "block_800", "safe", now, 1),
    ]
    client.insert(
        "chainlens.canonical_blocks",
        canonical_rows,
        column_names=[
            "chain_id", "block_number", "block_hash", "parent_hash",
            "security_level", "observed_at", "version",
        ],
    )
    client.insert(
        "chainlens.raw_transactions",
        [
            (
                chain_id, 800, "block_800", now, "fund_a", 0, funder, wallet_a,
                10, 21_000, 1, "0x", 1, now,
            ),
            (
                chain_id, 801, "block_801", now, "fund_b", 0, funder, wallet_b,
                10, 21_000, 1, "0x", 1, now,
            ),
        ],
        column_names=[
            "chain_id", "block_number", "block_hash", "block_timestamp", "tx_hash",
            "tx_index", "from_address", "to_address", "value_wei", "gas",
            "gas_price", "input", "status", "ingested_at",
        ],
    )

    store = ClickHouseClusteringStore(client)
    runner = ClusterRunner(store, window_seconds=3600, max_fanout=20)
    run_a = UUID(runner.run(chain_id, 800, 801).run_id)
    run_b = UUID(runner.run(chain_id, 800, 801).run_id)
    assert run_a != run_b

    for table, run_column, expected_rows in (
        ("wallet_clusters", "cluster_run_id", 2),
        ("entity_resolution_edges", "run_id", 1),
        ("cluster_evidence", "run_id", 3),
    ):
        counts = dict(
            client.query(
                f"SELECT {run_column}, count() FROM chainlens.{table} "
                "WHERE chain_id = {chain_id:UInt64} GROUP BY " + run_column,
                parameters={"chain_id": chain_id},
            ).result_rows
        )
        assert counts == {run_a: expected_rows, run_b: expected_rows}

    latest = client.query(
        "SELECT run_id FROM chainlens.latest_cluster_runs "
        "WHERE chain_id = {chain_id:UInt64} AND start_block = 800 "
        "AND end_block = 801 AND heuristic_version = 'shared_funder_v1'",
        parameters={"chain_id": chain_id},
    ).first_row[0]
    assert latest == run_b

    assert client.query(
        "SELECT DISTINCT cluster_run_id FROM chainlens.current_wallet_clusters "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(run_b,)]
    assert client.query(
        "SELECT DISTINCT run_id FROM chainlens.current_entity_resolution_edges "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(run_b,)]
    assert client.query(
        "SELECT DISTINCT run_id FROM chainlens.current_cluster_evidence "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(run_b,)]
    assert client.query(
        "SELECT count() FROM chainlens.wallet_clusters "
        "WHERE cluster_run_id = {run_id:UUID}",
        parameters={"run_id": str(run_a)},
    ).first_row[0] == 2

    run_c = uuid4()
    store.write_run_state(run_c, chain_id, 800, 801, "pending", now, 1)
    store.write_run_state(run_c, chain_id, 800, 801, "running", now, 2)
    store.write_run_state(
        run_c, chain_id, 800, 801, "failed", now, 3, error="deliberate test failure"
    )
    assert client.query(
        "SELECT run_id FROM chainlens.latest_cluster_runs "
        "WHERE chain_id = {chain_id:UInt64} AND start_block = 800 AND end_block = 801",
        parameters={"chain_id": chain_id},
    ).result_rows == [(run_b,)]


def test_label_snapshots_are_historical_and_failed_runs_do_not_become_current(
    client: Client, chain_id: int, tmp_path: Path
) -> None:
    """Repeated logical inputs retain history and only the newest success is current."""

    seed_path = tmp_path / "seeds.csv"
    with seed_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        writer.writerow(
            {
                "chain_id": chain_id,
                "address": "0x" + "ab" * 20,
                "entity_name": "Test Protocol",
                "entity_category": "protocol",
                "contract_role": "protocol_contract",
                "source_type": "test_fixture",
                "source_reference": "integration test",
                "confidence": 1.0,
                "notes": "known test contract",
            }
        )

    store = ClickHouseLabelStore(client)
    runner = LabelRunner(store, propagation_factor=0.95)
    first = runner.run(chain_id, seed_path)
    second = runner.run(chain_id, seed_path)
    assert first.run_id != second.run_id
    assert first.seed_dataset_version == second.seed_dataset_version
    assert (first.seed_count, first.direct_label_count, first.entity_count) == (1, 3, 1)

    historical = dict(
        client.query(
            "SELECT label_run_id, count() FROM chainlens.label_assignments "
            "WHERE chain_id = {chain_id:UInt64} GROUP BY label_run_id",
            parameters={"chain_id": chain_id},
        ).result_rows
    )
    assert historical == {UUID(first.run_id): 3, UUID(second.run_id): 3}
    assert client.query(
        "SELECT DISTINCT label_run_id FROM chainlens.current_label_assignments "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(UUID(second.run_id),)]

    failed_run = uuid4()
    now = datetime.now(UTC)
    store.write_run_state(
        failed_run,
        chain_id,
        None,
        second.taxonomy_version,
        second.seed_dataset_version,
        "pending",
        now,
        1,
    )
    store.write_run_state(
        failed_run,
        chain_id,
        None,
        second.taxonomy_version,
        second.seed_dataset_version,
        "failed",
        now,
        2,
        error="deliberate test failure",
    )
    assert client.query(
        "SELECT run_id FROM chainlens.latest_label_runs "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(UUID(second.run_id),)]


def test_evaluation_history_and_current_views_exclude_failed_runs(
    client: Client, chain_id: int, tmp_path: Path
) -> None:
    now = datetime.now(UTC)
    funder = "0x" + "11" * 20
    wallet_a = "0x" + "22" * 20
    wallet_b = "0x" + "33" * 20
    client.insert(
        "chainlens.canonical_blocks",
        [
            (chain_id, 900, "eval_900", "parent", "safe", now, 1),
            (chain_id, 901, "eval_901", "eval_900", "safe", now, 1),
        ],
        column_names=[
            "chain_id", "block_number", "block_hash", "parent_hash",
            "security_level", "observed_at", "version",
        ],
    )
    client.insert(
        "chainlens.raw_transactions",
        [
            (chain_id, 900, "eval_900", now, "eval_a", 0, funder, wallet_a,
             10, 21_000, 1, "0x", 1, now),
            (chain_id, 901, "eval_901", now, "eval_b", 0, funder, wallet_b,
             10, 21_000, 1, "0x", 1, now),
        ],
        column_names=[
            "chain_id", "block_number", "block_hash", "block_timestamp", "tx_hash",
            "tx_index", "from_address", "to_address", "value_wei", "gas",
            "gas_price", "input", "status", "ingested_at",
        ],
    )
    cluster_run_id = UUID(
        ClusterRunner(
            ClickHouseClusteringStore(client), window_seconds=3600, max_fanout=20
        ).run(chain_id, 900, 901).run_id
    )

    seed_path = tmp_path / "evaluation-seeds.csv"
    with seed_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        for wallet, role in ((wallet_a, "router"), (wallet_b, "factory")):
            writer.writerow({
                "chain_id": chain_id,
                "address": wallet,
                "entity_name": "Known Test Entity",
                "entity_category": "protocol",
                "contract_role": role,
                "source_type": "test_fixture",
                "source_reference": "evaluation integration test",
                "confidence": 1.0,
                "notes": role,
            })
    label_run_id = UUID(
        LabelRunner(ClickHouseLabelStore(client), propagation_factor=0.95).run(
            chain_id, seed_path, cluster_run_id=cluster_run_id
        ).run_id
    )
    store = ClickHouseEvaluationStore(client)
    runner = EvaluationRunner(store, thresholds=DriftThresholds())
    first = runner.run(chain_id, cluster_run_id, label_run_id, seed_path)
    second = runner.run(chain_id, cluster_run_id, label_run_id, seed_path)
    assert first.run_id != second.run_id
    assert first.clustering_evaluation.pairwise_f1 == 1.0
    assert first.ground_truth_version == second.ground_truth_version
    assert client.query(
        "SELECT uniqExact(evaluation_run_id) FROM chainlens.clustering_evaluations "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).first_row[0] == 2
    assert client.query(
        "SELECT DISTINCT evaluation_run_id FROM chainlens.current_clustering_evaluations "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(UUID(second.run_id),)]

    scope = store.read_scope(chain_id, cluster_run_id, label_run_id)
    failed = uuid4()
    store.write_run_state(
        failed, scope, second.ground_truth_version, EVALUATION_VERSION,
        "pending", now, 1,
    )
    store.write_run_state(
        failed, scope, second.ground_truth_version, EVALUATION_VERSION,
        "failed", now, 2, error="deliberate evaluation failure",
    )
    assert client.query(
        "SELECT run_id FROM chainlens.latest_evaluation_runs "
        "WHERE chain_id = {chain_id:UInt64}",
        parameters={"chain_id": chain_id},
    ).result_rows == [(UUID(second.run_id),)]
