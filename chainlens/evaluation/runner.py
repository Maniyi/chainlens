"""CLI orchestration for reproducible evaluation and drift snapshots."""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from chainlens.config import Settings, get_settings
from chainlens.db import create_client
from chainlens.evaluation.logic import (
    EVALUATION_VERSION,
    build_drift_metrics,
    compare_drift_metrics,
    evaluate_clustering,
    evaluate_labels,
    generate_drift_alerts,
)
from chainlens.evaluation.models import (
    ClusterEvaluation,
    DriftAlert,
    DriftMetric,
    DriftThresholds,
    EvaluatedLabel,
    EvaluationSummary,
    SnapshotScope,
    StoredLabelAssignment,
)
from chainlens.evaluation.store import ClickHouseEvaluationStore
from chainlens.labels.seeds import load_seed_file, seed_dataset_version


class EvaluationStore(Protocol):
    def read_scope(self, chain_id: int, cluster_run_id: UUID, label_run_id: UUID) -> SnapshotScope: ...
    def read_seen_addresses(self, scope: SnapshotScope) -> set[str]: ...
    def read_clusters(self, scope: SnapshotScope) -> tuple[dict[str, str], dict[str, float]]: ...
    def read_assignments(self, scope: SnapshotScope) -> list[StoredLabelAssignment]: ...
    def read_conflict_count(self, scope: SnapshotScope) -> int: ...
    def write_run_state(self, run_id: UUID, scope: SnapshotScope, ground_truth_version: str,
                        evaluation_version: str, status: str, started_at: datetime,
                        version: int, *, error: str | None = None) -> None: ...
    def read_previous_metrics(self, scope: SnapshotScope, ground_truth_version: str,
                              evaluation_version: str) -> dict[tuple[str, str, str], float]: ...
    def write_results(self, run_id: UUID, scope: SnapshotScope,
                      label_rows: list[EvaluatedLabel], cluster_row: ClusterEvaluation,
                      drift_rows: list[DriftMetric], alerts: list[DriftAlert],
                      evaluated_at: datetime) -> None: ...


class EvaluationRunner:
    def __init__(self, store: EvaluationStore, *, thresholds: DriftThresholds) -> None:
        self.store = store
        self.thresholds = thresholds

    def run(
        self,
        chain_id: int,
        cluster_run_id: UUID,
        label_run_id: UUID,
        ground_truth_file: str | Path,
    ) -> EvaluationSummary:
        if chain_id <= 0:
            raise ValueError("evaluation requires a positive chain_id")
        seeds = load_seed_file(ground_truth_file, chain_id=chain_id)
        ground_truth_version = seed_dataset_version(seeds)
        scope = self.store.read_scope(chain_id, cluster_run_id, label_run_id)
        run_id = uuid4()
        started_at = datetime.now(UTC)
        self.store.write_run_state(
            run_id, scope, ground_truth_version, EVALUATION_VERSION,
            "pending", started_at, 1,
        )
        self.store.write_run_state(
            run_id, scope, ground_truth_version, EVALUATION_VERSION,
            "running", started_at, 2,
        )
        try:
            seen = self.store.read_seen_addresses(scope)
            cluster_by_address, confidence_by_address = self.store.read_clusters(scope)
            assignments = self.store.read_assignments(scope)
            conflict_count = self.store.read_conflict_count(scope)
            label_rows = evaluate_labels(seeds, assignments)
            cluster_row = evaluate_clustering(seeds, seen, cluster_by_address)
            raw_drift = build_drift_metrics(
                wallet_count=scope.wallet_count,
                resolution_edge_count=scope.resolution_edge_count,
                cluster_by_address=cluster_by_address,
                cluster_confidence_by_address=confidence_by_address,
                entity_count=scope.label_entity_count,
                assignments=assignments,
                conflict_count=conflict_count,
                seeds=seeds,
                seen_addresses=seen,
            )
            previous = self.store.read_previous_metrics(
                scope, ground_truth_version, EVALUATION_VERSION
            )
            drift_rows = compare_drift_metrics(raw_drift, previous)
            alerts = generate_drift_alerts(drift_rows, self.thresholds)
            evaluated_at = datetime.now(UTC)
            self.store.write_results(
                run_id, scope, label_rows, cluster_row, drift_rows, alerts, evaluated_at
            )
            self.store.write_run_state(
                run_id, scope, ground_truth_version, EVALUATION_VERSION,
                "completed", started_at, 3,
            )
            return EvaluationSummary(
                str(run_id), chain_id, str(cluster_run_id), str(label_run_id),
                ground_truth_version, len(seeds), cluster_row.evaluated_address_count,
                tuple(label_rows), cluster_row, tuple(drift_rows), tuple(alerts),
            )
        except Exception as exc:
            self.store.write_run_state(
                run_id, scope, ground_truth_version, EVALUATION_VERSION,
                "failed", started_at, 3, error=str(exc)[:4000],
            )
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ChainLens snapshot evaluation")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="evaluate explicit cluster and label snapshots")
    run.add_argument("--chain-id", type=int, required=True)
    run.add_argument("--cluster-run-id", type=UUID, required=True)
    run.add_argument("--label-run-id", type=UUID, required=True)
    run.add_argument("--ground-truth", type=Path, required=True)
    return parser


def _print_summary(summary: EvaluationSummary) -> None:
    for key, value in (
        ("run_id", summary.run_id),
        ("chain_id", summary.chain_id),
        ("cluster_run_id", summary.cluster_run_id),
        ("label_run_id", summary.label_run_id),
        ("ground_truth_version", summary.ground_truth_version),
        ("ground_truth_addresses", summary.ground_truth_address_count),
        ("clustering_evaluated_addresses", summary.evaluated_address_count),
        ("clustering_tp_pairs", summary.clustering_evaluation.tp_pairs),
        ("clustering_fp_pairs", summary.clustering_evaluation.fp_pairs),
        ("clustering_fn_pairs", summary.clustering_evaluation.fn_pairs),
        ("clustering_pairwise_precision", summary.clustering_evaluation.pairwise_precision),
        ("clustering_pairwise_recall", summary.clustering_evaluation.pairwise_recall),
        ("clustering_pairwise_f1", summary.clustering_evaluation.pairwise_f1),
        ("drift_metric_count", len(summary.drift_metrics)),
        ("alert_count", len(summary.alerts)),
    ):
        print(f"{key}={value if value is not None else 'NULL'}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings: Settings = get_settings()
    client = create_client(settings)
    thresholds = DriftThresholds(
        float(settings.drift_max_largest_cluster_relative_increase),
        float(settings.drift_cluster_count_drop_threshold),
        float(settings.drift_propagated_label_spike_threshold),
        int(settings.drift_conflict_count_threshold),
        float(settings.drift_seed_coverage_drop_threshold),
    )
    try:
        summary = EvaluationRunner(
            ClickHouseEvaluationStore(client), thresholds=thresholds
        ).run(
            args.chain_id,
            args.cluster_run_id,
            args.label_run_id,
            args.ground_truth,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"evaluation failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
