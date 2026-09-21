"""ClickHouse schema and connectivity integration tests."""

from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from clickhouse_connect.driver.client import Client

from chainlens.db import create_client
from chainlens.evm.models import EvmBlock
from chainlens.ingestion.store import ClickHouseStore


BASE_TABLES = {
    "raw_blocks",
    "raw_transactions",
    "token_transfers",
    "canonical_blocks",
    "canonical_blocks_history",
    "reorg_events",
    "pipeline_runs",
    "pipeline_checkpoints",
}

VIEWS = {
    "current_canonical_blocks",
    "canonical_transactions",
    "canonical_token_transfers",
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
    assert BASE_TABLES <= names
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
