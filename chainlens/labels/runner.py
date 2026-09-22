"""Command-line orchestration for reproducible entity-label snapshots."""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from chainlens.config import Settings, get_settings
from chainlens.db import create_client
from chainlens.labels.logic import derive_label_snapshot
from chainlens.labels.models import ClusterMember, LabelRun, LabelRunSummary, LabelSnapshot
from chainlens.labels.seeds import load_seed_file, seed_dataset_version
from chainlens.labels.store import ClickHouseLabelStore
from chainlens.labels.taxonomy import TAXONOMY, TAXONOMY_VERSION, validate_taxonomy_version


class LabelStore(Protocol):
    def read_taxonomy(self, version: str) -> dict[str, set[str]]: ...
    def read_cluster_members(
        self, chain_id: int, cluster_run_id: UUID | None
    ) -> list[ClusterMember]: ...
    def get_run(self, run_id: UUID) -> LabelRun | None: ...
    def write_run_state(
        self, run_id: UUID, chain_id: int, cluster_run_id: UUID | None,
        taxonomy_version: str, dataset_version: str, status: str,
        started_at: datetime, version: int, **counts: object,
    ) -> None: ...
    def write_snapshot(
        self, run_id: UUID, snapshot: LabelSnapshot, derived_at: datetime
    ) -> None: ...
    def delete_partial_run_outputs(self, run_id: UUID) -> None: ...


class LabelRunner:
    def __init__(self, store: LabelStore, *, propagation_factor: float = 0.95) -> None:
        self.store = store
        self.propagation_factor = propagation_factor

    def run(
        self,
        chain_id: int,
        seed_file: str | Path,
        taxonomy_version: str = TAXONOMY_VERSION,
        *,
        cluster_run_id: UUID | None = None,
        resume_run_id: UUID | None = None,
    ) -> LabelRunSummary:
        if chain_id <= 0:
            raise ValueError("labeling requires a positive chain_id")
        validate_taxonomy_version(taxonomy_version)
        if not 0.0 <= self.propagation_factor <= 1.0:
            raise ValueError("propagation factor must be in [0,1]")
        seeds = load_seed_file(seed_file, chain_id=chain_id)
        dataset_version = seed_dataset_version(seeds)
        stored_taxonomy = self.store.read_taxonomy(taxonomy_version)
        if stored_taxonomy != {key: set(value) for key, value in TAXONOMY.items()}:
            raise ValueError(f"warehouse taxonomy {taxonomy_version} is missing or inconsistent")
        cluster_members = self.store.read_cluster_members(chain_id, cluster_run_id)

        run_id = resume_run_id or uuid4()
        started_at = datetime.now(UTC)
        initial_version = 1
        if resume_run_id is not None:
            previous = self.store.get_run(run_id)
            if previous is None:
                raise ValueError(f"label run {run_id} does not exist")
            if previous.status == "completed":
                raise ValueError(f"label run {run_id} is already completed")
            if (
                previous.chain_id,
                previous.cluster_run_id,
                previous.taxonomy_version,
                previous.seed_dataset_version,
            ) != (chain_id, cluster_run_id, taxonomy_version, dataset_version):
                raise ValueError("resume arguments do not match the original label run")
            started_at = previous.started_at
            initial_version = previous.version + 1
            self.store.delete_partial_run_outputs(run_id)

        self.store.write_run_state(
            run_id, chain_id, cluster_run_id, taxonomy_version, dataset_version,
            "pending", started_at, initial_version,
        )
        self.store.write_run_state(
            run_id, chain_id, cluster_run_id, taxonomy_version, dataset_version,
            "running", started_at, initial_version + 1,
        )
        try:
            snapshot = derive_label_snapshot(
                chain_id,
                seeds,
                cluster_members,
                taxonomy_version=taxonomy_version,
                propagation_factor=self.propagation_factor,
            )
            self.store.write_snapshot(run_id, snapshot, datetime.now(UTC))
            direct_count = sum(
                item.assignment_method == "direct_seed" for item in snapshot.assignments
            )
            propagated_count = sum(
                item.assignment_method == "cluster_propagation"
                for item in snapshot.assignments
            )
            self.store.write_run_state(
                run_id, chain_id, cluster_run_id, taxonomy_version, dataset_version,
                "completed", started_at, initial_version + 2,
                seed_count=len(seeds), direct_label_count=direct_count,
                propagated_label_count=propagated_count,
                entity_count=len(snapshot.entities),
            )
            return LabelRunSummary(
                str(run_id), chain_id, str(cluster_run_id) if cluster_run_id else None,
                taxonomy_version, dataset_version, len(seeds), direct_count,
                propagated_count, len(snapshot.entities), len(snapshot.conflicts),
            )
        except Exception as exc:
            self.store.delete_partial_run_outputs(run_id)
            self.store.write_run_state(
                run_id, chain_id, cluster_run_id, taxonomy_version, dataset_version,
                "failed", started_at, initial_version + 3, error=str(exc)[:4000],
            )
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ChainLens entity labeling")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="build one immutable label snapshot")
    run.add_argument("--chain-id", type=int, required=True)
    run.add_argument("--cluster-run-id", type=UUID)
    run.add_argument("--seed-file", type=Path, required=True)
    run.add_argument("--taxonomy-version", default=TAXONOMY_VERSION)
    run.add_argument("--resume-run-id", type=UUID)
    return parser


def _print_summary(summary: LabelRunSummary) -> None:
    for key, value in (
        ("run_id", summary.run_id),
        ("chain_id", summary.chain_id),
        ("cluster_run_id", summary.cluster_run_id or "none"),
        ("taxonomy_version", summary.taxonomy_version),
        ("seed_dataset_version", summary.seed_dataset_version),
        ("seeds", summary.seed_count),
        ("direct_labels", summary.direct_label_count),
        ("propagated_labels", summary.propagated_label_count),
        ("entities", summary.entity_count),
        ("conflicts", summary.conflict_count),
    ):
        print(f"{key}={value}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings: Settings = get_settings()
    client = create_client(settings)
    try:
        summary = LabelRunner(
            ClickHouseLabelStore(client),
            propagation_factor=float(settings.label_propagation_factor),
        ).run(
            args.chain_id,
            args.seed_file,
            args.taxonomy_version,
            cluster_run_id=args.cluster_run_id,
            resume_run_id=args.resume_run_id,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"labeling failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
