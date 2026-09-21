"""Command-line orchestration for bounded, restartable clustering runs."""

import argparse
import sys
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from chainlens.clustering.logic import HEURISTIC_VERSION, derive_snapshot
from chainlens.clustering.models import (
    CanonicalTokenTransfer,
    CanonicalTransaction,
    ClusterRun,
    ClusterRunSummary,
    DerivedSnapshot,
)
from chainlens.clustering.store import ClickHouseClusteringStore
from chainlens.config import Settings, get_settings
from chainlens.db import create_client


class ClusteringStore(Protocol):
    def read_canonical_transactions(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalTransaction]: ...

    def read_canonical_block_versions(
        self, chain_id: int, start_block: int, end_block: int
    ) -> tuple[tuple[int, str, int], ...]: ...

    def read_canonical_token_transfers(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalTokenTransfer]: ...

    def get_run(self, run_id: UUID) -> ClusterRun | None: ...

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
    ) -> None: ...

    def write_snapshot(
        self, run_id: UUID, snapshot: DerivedSnapshot, derived_at: datetime
    ) -> None: ...

    def delete_partial_run_outputs(self, run_id: UUID) -> None: ...


class ClusterRunner:
    def __init__(
        self, store: ClusteringStore, *, window_seconds: int, max_fanout: int
    ) -> None:
        self.store = store
        self.window_seconds = window_seconds
        self.max_fanout = max_fanout

    def run(
        self,
        chain_id: int,
        start_block: int,
        end_block: int,
        *,
        resume_run_id: UUID | None = None,
    ) -> ClusterRunSummary:
        if chain_id < 0 or start_block < 0 or end_block < start_block:
            raise ValueError(
                "clustering requires a non-negative chain and bounded block range"
            )
        if self.window_seconds <= 0 or self.max_fanout <= 0:
            raise ValueError("shared-funder configuration must be positive")

        run_id = resume_run_id or uuid4()
        started_at = datetime.now(UTC)
        initial_version = 1
        if resume_run_id is not None:
            previous = self.store.get_run(run_id)
            if previous is None:
                raise ValueError(f"cluster run {run_id} does not exist")
            if previous.status == "completed":
                raise ValueError(f"cluster run {run_id} is already completed")
            if (
                previous.chain_id,
                previous.start_block,
                previous.end_block,
                previous.heuristic_version,
            ) != (chain_id, start_block, end_block, HEURISTIC_VERSION):
                raise ValueError("resume arguments do not match the original cluster run")
            started_at = previous.started_at
            initial_version = previous.version + 1
            self.store.delete_partial_run_outputs(run_id)
        self.store.write_run_state(
            run_id, chain_id, start_block, end_block, "pending", started_at, initial_version
        )
        self.store.write_run_state(
            run_id, chain_id, start_block, end_block, "running", started_at, initial_version + 1
        )
        try:
            canonical_before = self.store.read_canonical_block_versions(
                chain_id, start_block, end_block
            )
            transactions = self.store.read_canonical_transactions(
                chain_id, start_block, end_block
            )
            token_transfers = self.store.read_canonical_token_transfers(
                chain_id, start_block, end_block
            )
            snapshot = derive_snapshot(
                chain_id,
                transactions,
                token_transfers,
                window_seconds=self.window_seconds,
                max_fanout=self.max_fanout,
            )
            canonical_after = self.store.read_canonical_block_versions(
                chain_id, start_block, end_block
            )
            if canonical_after != canonical_before:
                raise RuntimeError(
                    "canonical block state changed during clustering; rerun the range"
                )
            self.store.write_snapshot(run_id, snapshot, datetime.now(UTC))
            cluster_ids = {member.cluster_id for member in snapshot.clusters}
            self.store.write_run_state(
                run_id,
                chain_id,
                start_block,
                end_block,
                "completed",
                started_at,
                initial_version + 2,
                wallet_count=len(snapshot.wallet_features),
                resolution_edge_count=len(snapshot.resolution_edges),
                cluster_count=len(cluster_ids),
            )
            return ClusterRunSummary(
                str(run_id),
                chain_id,
                start_block,
                end_block,
                len(snapshot.transfer_edges),
                len(snapshot.funding_edges),
                len(snapshot.wallet_features),
                len(snapshot.resolution_edges),
                len(cluster_ids),
                max((member.cluster_size for member in snapshot.clusters), default=0),
            )
        except Exception as exc:
            self.store.delete_partial_run_outputs(run_id)
            self.store.write_run_state(
                run_id,
                chain_id,
                start_block,
                end_block,
                "failed",
                started_at,
                initial_version + 3,
                error=str(exc)[:4000],
            )
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ChainLens wallet clustering")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="cluster one explicit canonical block range")
    run.add_argument("--chain-id", type=int, required=True)
    run.add_argument("--from-block", type=int, required=True)
    run.add_argument("--to-block", type=int, required=True)
    run.add_argument("--resume-run-id", type=UUID)
    return parser


def _print_summary(summary: ClusterRunSummary) -> None:
    for key, value in (
        ("run_id", summary.run_id),
        ("chain_id", summary.chain_id),
        ("range", f"{summary.start_block}-{summary.end_block}"),
        ("transfer_edges", summary.transfer_edge_count),
        ("funding_edges", summary.funding_edge_count),
        ("wallet_features", summary.wallet_feature_count),
        ("resolution_edges", summary.resolution_edge_count),
        ("clusters", summary.cluster_count),
        ("largest_cluster_size", summary.largest_cluster_size),
    ):
        print(f"{key}={value}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings: Settings = get_settings()
    client = create_client(settings)
    try:
        summary = ClusterRunner(
            ClickHouseClusteringStore(client),
            window_seconds=int(settings.cluster_shared_funder_window_seconds),
            max_fanout=int(settings.cluster_max_funder_fanout),
        ).run(
            args.chain_id,
            args.from_block,
            args.to_block,
            resume_run_id=args.resume_run_id,
        )
    except (ValueError, RuntimeError) as exc:
        print(f"clustering failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
