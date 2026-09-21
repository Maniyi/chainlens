"""ClickHouse reads and writes for bounded clustering snapshots."""

from datetime import UTC, datetime
from uuid import UUID

from clickhouse_connect.driver.client import Client

from chainlens.clustering.logic import HEURISTIC_VERSION
from chainlens.clustering.models import (
    CanonicalTokenTransfer,
    CanonicalTransaction,
    ClusterRun,
    DerivedSnapshot,
)


class ClickHouseClusteringStore:
    """Persist complete run-scoped snapshots and versioned run state."""

    OUTPUT_TABLES = (
        ("wallet_transfer_edges", "run_id"),
        ("wallet_funding_edges", "run_id"),
        ("wallet_features", "run_id"),
        ("entity_resolution_edges", "run_id"),
        ("cluster_evidence", "run_id"),
        ("wallet_clusters", "cluster_run_id"),
    )

    def __init__(self, client: Client) -> None:
        self.client = client

    def read_canonical_transactions(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalTransaction]:
        rows = self.client.query(
            "SELECT chain_id, block_number, block_hash, block_timestamp, tx_hash, "
            "tx_index, from_address, to_address, value_wei, status "
            "FROM chainlens.canonical_transactions "
            "WHERE chain_id = {chain_id:UInt64} "
            "AND block_number BETWEEN {start:UInt64} AND {end:UInt64} "
            "ORDER BY ingested_at DESC LIMIT 1 BY chain_id, block_hash, tx_hash",
            parameters={"chain_id": chain_id, "start": start_block, "end": end_block},
        ).result_rows
        return sorted(
            (CanonicalTransaction(*row) for row in rows),
            key=lambda tx: (tx.block_number, tx.tx_index, tx.tx_hash),
        )

    def read_canonical_block_versions(
        self, chain_id: int, start_block: int, end_block: int
    ) -> tuple[tuple[int, str, int], ...]:
        """Capture reorg-sensitive coordinates for an optimistic snapshot check."""

        rows = self.client.query(
            "SELECT block_number, block_hash, version "
            "FROM chainlens.current_canonical_blocks "
            "WHERE chain_id = {chain_id:UInt64} "
            "AND block_number BETWEEN {start:UInt64} AND {end:UInt64} "
            "ORDER BY block_number",
            parameters={"chain_id": chain_id, "start": start_block, "end": end_block},
        ).result_rows
        return tuple((row[0], row[1], row[2]) for row in rows)

    def read_canonical_token_transfers(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalTokenTransfer]:
        rows = self.client.query(
            "SELECT chain_id, block_number, block_hash, block_timestamp, tx_hash, "
            "log_index, token_address, from_address, to_address, amount_raw "
            "FROM chainlens.canonical_token_transfers "
            "WHERE chain_id = {chain_id:UInt64} "
            "AND block_number BETWEEN {start:UInt64} AND {end:UInt64} "
            "ORDER BY ingested_at DESC "
            "LIMIT 1 BY chain_id, block_hash, tx_hash, log_index",
            parameters={"chain_id": chain_id, "start": start_block, "end": end_block},
        ).result_rows
        return sorted(
            (CanonicalTokenTransfer(*row) for row in rows),
            key=lambda transfer: (
                transfer.block_number,
                transfer.tx_hash,
                transfer.log_index,
            ),
        )

    def get_run(self, run_id: UUID) -> ClusterRun | None:
        rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3), tupleElement(state, 4), tupleElement(state, 5), "
            "tupleElement(state, 6), max_version FROM ("
            "SELECT argMax(tuple(chain_id, start_block, end_block, heuristic_version, "
            "status, started_at), version) AS state, max(version) AS max_version "
            "FROM chainlens.cluster_runs WHERE run_id = {run_id:UUID}) "
            "WHERE max_version > 0",
            parameters={"run_id": str(run_id)},
        ).result_rows
        if not rows:
            return None
        row = rows[0]
        return ClusterRun(
            run_id, row[0], row[1], row[2], row[3], row[4], row[5], row[6]
        )

    def write_run_state(
        self,
        run_id: UUID,
        chain_id: int,
        start_block: int,
        end_block: int,
        status: str,
        started_at: datetime,
        version: int,
        *,
        wallet_count: int = 0,
        resolution_edge_count: int = 0,
        cluster_count: int = 0,
        error: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        self.client.insert(
            "chainlens.cluster_runs",
            [
                (
                    run_id,
                    chain_id,
                    start_block,
                    end_block,
                    HEURISTIC_VERSION,
                    status,
                    started_at,
                    now if status in {"completed", "failed"} else None,
                    wallet_count,
                    resolution_edge_count,
                    cluster_count,
                    error,
                    now,
                    version,
                )
            ],
            column_names=[
                "run_id",
                "chain_id",
                "start_block",
                "end_block",
                "heuristic_version",
                "status",
                "started_at",
                "completed_at",
                "wallet_count",
                "resolution_edge_count",
                "cluster_count",
                "error",
                "updated_at",
                "version",
            ],
        )

    def write_snapshot(
        self, run_id: UUID, snapshot: DerivedSnapshot, derived_at: datetime
    ) -> None:
        if snapshot.transfer_edges:
            self.client.insert(
                "chainlens.wallet_transfer_edges",
                [
                    (
                        edge.chain_id,
                        edge.block_number,
                        edge.block_hash,
                        edge.block_timestamp,
                        edge.tx_hash,
                        edge.asset_type,
                        edge.token_address,
                        edge.from_address,
                        edge.to_address,
                        edge.amount_raw,
                        edge.edge_id,
                        derived_at,
                        run_id,
                    )
                    for edge in snapshot.transfer_edges
                ],
                column_names=[
                    "chain_id", "block_number", "block_hash", "block_timestamp",
                    "tx_hash", "asset_type", "token_address", "from_address",
                    "to_address", "amount_raw", "edge_id", "derived_at", "run_id",
                ],
            )
        if snapshot.funding_edges:
            self.client.insert(
                "chainlens.wallet_funding_edges",
                [
                    (
                        edge.chain_id, edge.funded_address, edge.funder_address,
                        edge.funding_tx_hash, edge.funding_block_number,
                        edge.funding_block_hash, edge.funding_timestamp, edge.amount_wei,
                        edge.heuristic, edge.confidence, run_id, derived_at,
                    )
                    for edge in snapshot.funding_edges
                ],
                column_names=[
                    "chain_id", "funded_address", "funder_address", "funding_tx_hash",
                    "funding_block_number", "funding_block_hash", "funding_timestamp",
                    "amount_wei", "heuristic", "confidence", "run_id", "derived_at",
                ],
            )
        if snapshot.wallet_features:
            self.client.insert(
                "chainlens.wallet_features",
                [
                    (
                        feature.chain_id, feature.wallet_address, feature.first_seen_block,
                        feature.last_seen_block, feature.incoming_native_tx_count,
                        feature.outgoing_native_tx_count,
                        feature.incoming_erc20_transfer_count,
                        feature.outgoing_erc20_transfer_count,
                        feature.unique_counterparties, feature.native_received_wei,
                        feature.native_sent_wei, feature.first_funder_address,
                        feature.first_funding_block, run_id, derived_at,
                    )
                    for feature in snapshot.wallet_features
                ],
                column_names=[
                    "chain_id", "wallet_address", "first_seen_block", "last_seen_block",
                    "incoming_native_tx_count", "outgoing_native_tx_count",
                    "incoming_erc20_transfer_count", "outgoing_erc20_transfer_count",
                    "unique_counterparties", "native_received_wei", "native_sent_wei",
                    "first_funder_address", "first_funding_block", "run_id", "derived_at",
                ],
            )
        if snapshot.resolution_edges:
            self.client.insert(
                "chainlens.entity_resolution_edges",
                [
                    (
                        edge.chain_id, edge.wallet_a, edge.wallet_b, edge.heuristic,
                        edge.score, edge.evidence_count, run_id, derived_at, edge.edge_id,
                    )
                    for edge in snapshot.resolution_edges
                ],
                column_names=[
                    "chain_id", "wallet_a", "wallet_b", "heuristic", "score",
                    "evidence_count", "run_id", "derived_at", "edge_id",
                ],
            )
        if snapshot.evidence:
            self.client.insert(
                "chainlens.cluster_evidence",
                [
                    (
                        item.chain_id, item.wallet_a, item.wallet_b, item.heuristic,
                        item.evidence_type, item.evidence_value, item.weight,
                        item.score_contribution, run_id, derived_at,
                    )
                    for item in snapshot.evidence
                ],
                column_names=[
                    "chain_id", "wallet_a", "wallet_b", "heuristic", "evidence_type",
                    "evidence_value", "weight", "score_contribution", "run_id", "derived_at",
                ],
            )
        if snapshot.clusters:
            self.client.insert(
                "chainlens.wallet_clusters",
                [
                    (
                        member.chain_id, member.cluster_id, member.wallet_address, run_id,
                        member.cluster_size, member.confidence, HEURISTIC_VERSION, derived_at,
                    )
                    for member in snapshot.clusters
                ],
                column_names=[
                    "chain_id", "cluster_id", "wallet_address", "cluster_run_id",
                    "cluster_size", "confidence", "heuristic_version", "derived_at",
                ],
            )

    def delete_partial_run_outputs(self, run_id: UUID) -> None:
        """Remove incomplete output for this run only; completed runs are immutable."""

        for table, column in self.OUTPUT_TABLES:
            self.client.command(
                f"ALTER TABLE chainlens.{table} DELETE WHERE {column} = {{run_id:UUID}} "
                "SETTINGS mutations_sync = 1",
                parameters={"run_id": str(run_id)},
            )
