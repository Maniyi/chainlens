"""Focused deterministic tests for the Milestone 3 wallet-intelligence layer."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from chainlens.clustering.logic import (
    canonical_pair,
    connected_components,
    derive_clusters,
    derive_first_funders,
    derive_shared_funder_edges,
    derive_snapshot,
    derive_transfer_edges,
)
from chainlens.clustering.models import (
    CanonicalTokenTransfer,
    CanonicalTransaction,
    FundingEdge,
    ResolutionEdge,
)
from chainlens.clustering.runner import ClusterRunner


CHAIN_ID = 8453
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def address(value: int) -> str:
    return f"0x{value:040x}"


def transaction(
    sender: int,
    recipient: int,
    *,
    block: int,
    index: int,
    tx_marker: int,
    value: int = 1,
    block_hash_marker: int | None = None,
) -> CanonicalTransaction:
    return CanonicalTransaction(
        CHAIN_ID,
        block,
        f"0x{(block_hash_marker or block):064x}",
        NOW + timedelta(seconds=block),
        f"0x{tx_marker:064x}",
        index,
        address(sender),
        address(recipient),
        value,
        1,
    )


def funding(funder: int, funded: int, seconds: int) -> FundingEdge:
    return FundingEdge(
        CHAIN_ID,
        address(funded),
        address(funder),
        f"0x{funded:064x}",
        100 + seconds,
        f"0x{(100 + seconds):064x}",
        NOW + timedelta(seconds=seconds),
        10,
    )


def test_transfer_edge_ids_are_deterministic_and_asset_specific() -> None:
    tx = transaction(1, 2, block=100, index=0, tx_marker=10)
    token = CanonicalTokenTransfer(
        CHAIN_ID,
        100,
        tx.block_hash,
        tx.block_timestamp,
        tx.tx_hash,
        7,
        address(99),
        address(1),
        address(2),
        55,
    )
    first = derive_transfer_edges([tx], [token])
    second = derive_transfer_edges([tx], [token])

    assert [edge.edge_id for edge in first] == [edge.edge_id for edge in second]
    assert len({edge.edge_id for edge in first}) == 2
    assert {edge.asset_type for edge in first} == {"native", "erc20"}


def test_first_native_funder_uses_earliest_transaction() -> None:
    edges = derive_transfer_edges(
        [
            transaction(1, 3, block=100, index=0, tx_marker=1),
            transaction(2, 3, block=101, index=0, tx_marker=2),
        ],
        [],
    )
    assert derive_first_funders(edges)[0].funder_address == address(1)


def test_first_funder_tie_breaks_by_transaction_index() -> None:
    edges = derive_transfer_edges(
        [
            transaction(1, 3, block=100, index=9, tx_marker=1),
            transaction(2, 3, block=100, index=2, tx_marker=2),
        ],
        [],
    )
    selected = derive_first_funders(edges)[0]
    assert selected.funder_address == address(2)
    assert selected.funding_tx_hash == f"0x{2:064x}"


def test_shared_funder_generates_all_canonical_pairs_and_evidence() -> None:
    resolution, evidence = derive_shared_funder_edges(
        [funding(9, 1, 0), funding(9, 2, 10), funding(9, 3, 20)],
        window_seconds=60,
        max_fanout=10,
    )
    assert {(edge.wallet_a, edge.wallet_b) for edge in resolution} == {
        (address(1), address(2)),
        (address(1), address(3)),
        (address(2), address(3)),
    }
    assert len({edge.edge_id for edge in resolution}) == 3
    assert len(evidence) == 9
    for edge in resolution:
        types = {
            item.evidence_type
            for item in evidence
            if (item.wallet_a, item.wallet_b) == (edge.wallet_a, edge.wallet_b)
        }
        assert types == {
            "same_first_funder",
            "funding_time_delta_seconds",
            "funder_fanout",
        }
        assert edge.score == sum(
            item.score_contribution
            for item in evidence
            if (item.wallet_a, item.wallet_b) == (edge.wallet_a, edge.wallet_b)
        )


def test_pair_canonicalization_has_one_representation() -> None:
    assert canonical_pair(address(2), address(1)) == canonical_pair(
        address(1), address(2)
    )


def test_fanout_guard_excludes_large_funder() -> None:
    edges, evidence = derive_shared_funder_edges(
        [funding(9, wallet, wallet) for wallet in range(1, 5)],
        window_seconds=60,
        max_fanout=3,
    )
    assert edges == []
    assert evidence == []


def test_time_window_guard_excludes_distant_funding() -> None:
    edges, _ = derive_shared_funder_edges(
        [funding(9, 1, 0), funding(9, 2, 61)],
        window_seconds=60,
        max_fanout=3,
    )
    assert edges == []


def test_connected_components_and_cluster_id_are_deterministic() -> None:
    def edge(left: int, right: int) -> ResolutionEdge:
        wallet_a, wallet_b = canonical_pair(address(left), address(right))
        return ResolutionEdge(CHAIN_ID, wallet_a, wallet_b, "shared_funder", 0.8, 3, f"{left}-{right}")

    edges = [edge(1, 2), edge(2, 3), edge(4, 5)]
    assert connected_components(edges) == [
        (address(1), address(2), address(3)),
        (address(4), address(5)),
    ]
    first = derive_clusters(CHAIN_ID, edges)
    second = derive_clusters(CHAIN_ID, list(reversed(edges)))
    assert {(item.wallet_address, item.cluster_id) for item in first} == {
        (item.wallet_address, item.cluster_id) for item in second
    }


def test_rerun_has_same_logical_edges_clusters_and_no_singletons() -> None:
    facts = [
        transaction(9, 1, block=100, index=0, tx_marker=1),
        transaction(9, 2, block=101, index=0, tx_marker=2),
        transaction(8, 7, block=102, index=0, tx_marker=3),
    ]
    first = derive_snapshot(CHAIN_ID, facts, [], window_seconds=60, max_fanout=5)
    second = derive_snapshot(CHAIN_ID, list(reversed(facts)), [], window_seconds=60, max_fanout=5)

    assert [edge.edge_id for edge in first.transfer_edges] == [
        edge.edge_id for edge in second.transfer_edges
    ]
    assert [edge.edge_id for edge in first.resolution_edges] == [
        edge.edge_id for edge in second.resolution_edges
    ]
    assert {(item.wallet_address, item.cluster_id) for item in first.clusters} == {
        (item.wallet_address, item.cluster_id) for item in second.clusters
    }
    assert address(7) not in {item.wallet_address for item in first.clusters}


def test_run_fails_if_canonical_state_changes_during_reads() -> None:
    class ChangingStore:
        def __init__(self) -> None:
            self.snapshots = iter(
                [((100, "old", 1),), ((100, "replacement", 2),)]
            )
            self.states: list[str] = []

        def read_canonical_block_versions(self, chain_id, start_block, end_block):
            return next(self.snapshots)

        def read_canonical_transactions(self, chain_id, start_block, end_block):
            return []

        def read_canonical_token_transfers(self, chain_id, start_block, end_block):
            return []

        def get_run(self, run_id):
            return None

        def write_run_state(
            self, run_id, chain_id, start_block, end_block, status, started_at,
            version, **kwargs
        ):
            self.states.append(status)

        def write_snapshot(self, run_id, snapshot, derived_at):
            raise AssertionError("a mixed canonical snapshot must not be written")

        def delete_partial_run_outputs(self, run_id: UUID):
            return None

    store = ChangingStore()
    runner = ClusterRunner(store, window_seconds=60, max_fanout=5)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="canonical block state changed"):
        runner.run(CHAIN_ID, 100, 100)
    assert store.states == ["pending", "running", "failed"]
