"""Immutable models shared by evaluation logic, persistence, and the CLI."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class MetricValues:
    precision: float | None
    recall: float | None
    f1: float | None


@dataclass(frozen=True)
class StoredLabelAssignment:
    subject_type: str
    subject_id: str
    entity_id: str | None
    label_dimension: str
    label_value: str
    confidence: float
    assignment_method: str


@dataclass(frozen=True)
class EvaluatedLabel:
    label_dimension: str
    label_value: str
    assignment_method: str
    tp: int
    fp: int
    fn: int
    precision: float | None
    recall: float | None
    f1: float | None
    support: int
    predicted_count: int
    coverage: float


@dataclass(frozen=True)
class ClusterEvaluation:
    metric_scope: str
    tp_pairs: int
    fp_pairs: int
    fn_pairs: int
    pairwise_precision: float | None
    pairwise_recall: float | None
    pairwise_f1: float | None
    ground_truth_entity_count: int
    ground_truth_address_count: int
    evaluated_address_count: int
    predicted_cluster_count: int


@dataclass(frozen=True)
class SnapshotScope:
    chain_id: int
    cluster_run_id: UUID
    label_run_id: UUID
    start_block: int
    end_block: int
    heuristic_version: str
    taxonomy_version: str
    seed_dataset_version: str
    wallet_count: int
    resolution_edge_count: int
    cluster_count: int
    label_entity_count: int
    direct_label_count: int
    propagated_label_count: int


@dataclass(frozen=True)
class EvaluationRun:
    run_id: UUID
    scope: SnapshotScope
    ground_truth_version: str
    evaluation_version: str
    status: str
    started_at: datetime
    version: int


@dataclass(frozen=True)
class DriftMetric:
    metric_group: str
    metric_name: str
    scope_key: str
    value: float
    previous_value: float | None = None
    absolute_change: float | None = None
    relative_change: float | None = None


@dataclass(frozen=True)
class DriftAlert:
    metric_name: str
    scope_key: str
    severity: str
    current_value: float
    previous_value: float | None
    threshold_type: str
    threshold_value: float
    message: str


@dataclass(frozen=True)
class DriftThresholds:
    largest_cluster_relative_increase: float = 1.0
    cluster_count_drop: float = 0.5
    propagated_label_spike: float = 1.0
    conflict_count: int = 1
    seed_coverage_drop: float = 0.2


@dataclass(frozen=True)
class EvaluationSummary:
    run_id: str
    chain_id: int
    cluster_run_id: str
    label_run_id: str
    ground_truth_version: str
    ground_truth_address_count: int
    evaluated_address_count: int
    label_evaluations: tuple[EvaluatedLabel, ...]
    clustering_evaluation: ClusterEvaluation
    drift_metrics: tuple[DriftMetric, ...]
    alerts: tuple[DriftAlert, ...]
