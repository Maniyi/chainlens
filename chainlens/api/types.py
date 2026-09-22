"""Explicit Strawberry types exposed by the read API."""

from dataclasses import field
from datetime import datetime
from typing import Callable
from uuid import UUID

import strawberry


@strawberry.type
class SnapshotMetadata:
    cluster_run_id: UUID | None = None
    label_run_id: UUID | None = None
    evaluation_run_id: UUID | None = None
    heuristic_version: str | None = None
    taxonomy_version: str | None = None
    ground_truth_version: str | None = None
    evaluation_version: str | None = None


@strawberry.type
class LabelEvidence:
    type: str
    value: str
    weight: float
    confidence_contribution: float
    source_type: str
    source_reference: str


@strawberry.type
class LabelAssignment:
    dimension: str
    value: str
    confidence: float
    assignment_method: str
    source_type: str
    source_reference: str
    taxonomy_version: str
    evidence: list[LabelEvidence] = field(default_factory=list)


@strawberry.type
class WalletFeatures:
    first_seen_block: int
    last_seen_block: int
    incoming_native_tx_count: int
    outgoing_native_tx_count: int
    incoming_erc20_transfer_count: int
    outgoing_erc20_transfer_count: int
    unique_counterparties: int
    native_received_wei: str
    native_sent_wei: str
    first_funder_address: str | None
    first_funding_block: int | None


@strawberry.type
class EntityMember:
    address: str
    membership_method: str
    membership_confidence: float
    cluster_id: str | None = None


@strawberry.type
class ClusterMember:
    address: str
    confidence: float


@strawberry.type
class ResolutionEdge:
    wallet_a: str
    wallet_b: str
    heuristic: str
    score: float
    evidence_count: int


@strawberry.type
class ClusterEvidence:
    wallet_a: str
    wallet_b: str
    heuristic: str
    evidence_type: str
    evidence_value: str
    weight: float
    score_contribution: float


@strawberry.type
class Entity:
    id: str
    display_name: str | None
    category: str
    membership_method: str | None
    membership_confidence: float | None
    labels: list[LabelAssignment]
    evidence: list[LabelEvidence]
    cluster_id: str | None
    snapshot: SnapshotMetadata
    _members_loader: strawberry.Private[Callable[[int, int], list[EntityMember]] | None] = None

    @strawberry.field
    def members(self, limit: int = 50, offset: int = 0) -> list[EntityMember]:
        from chainlens.api.resolvers import pagination

        bounded, start = pagination(limit, offset)
        return self._members_loader(bounded, start) if self._members_loader else []


@strawberry.type
class Cluster:
    id: str
    size: int
    confidence: float
    heuristic_version: str
    snapshot: SnapshotMetadata
    _members_loader: strawberry.Private[Callable[[int, int], list[ClusterMember]] | None] = None
    _edges_loader: strawberry.Private[Callable[[int, int], list[ResolutionEdge]] | None] = None
    _evidence_loader: strawberry.Private[Callable[[int, int], list[ClusterEvidence]] | None] = None

    @strawberry.field
    def members(self, limit: int = 50, offset: int = 0) -> list[ClusterMember]:
        from chainlens.api.resolvers import pagination

        bounded, start = pagination(limit, offset)
        return self._members_loader(bounded, start) if self._members_loader else []

    @strawberry.field(name="resolutionEdges")
    def resolution_edges(self, limit: int = 50, offset: int = 0) -> list[ResolutionEdge]:
        from chainlens.api.resolvers import pagination

        bounded, start = pagination(limit, offset)
        return self._edges_loader(bounded, start) if self._edges_loader else []

    @strawberry.field
    def edges(self, limit: int = 50, offset: int = 0) -> list[ResolutionEdge]:
        from chainlens.api.resolvers import pagination

        bounded, start = pagination(limit, offset)
        return self._edges_loader(bounded, start) if self._edges_loader else []

    @strawberry.field
    def evidence(self, limit: int = 50, offset: int = 0) -> list[ClusterEvidence]:
        from chainlens.api.resolvers import pagination

        bounded, start = pagination(limit, offset)
        return self._evidence_loader(bounded, start) if self._evidence_loader else []


@strawberry.type
class Wallet:
    address: str
    chain_id: int
    features: WalletFeatures | None
    entity: Entity | None
    cluster: Cluster | None
    labels: list[LabelAssignment]
    snapshot: SnapshotMetadata


@strawberry.type
class LabelEvaluation:
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


@strawberry.type
class ClusteringEvaluation:
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


@strawberry.type
class DriftMetric:
    metric_group: str
    metric_name: str
    scope_key: str
    value: float
    previous_value: float | None
    absolute_change: float | None
    relative_change: float | None


@strawberry.type
class DriftAlert:
    severity: str
    metric_name: str
    scope_key: str
    current_value: float
    previous_value: float | None
    threshold_type: str
    threshold_value: float
    message: str
    created_at: datetime


@strawberry.type
class EvaluationSummary:
    run_id: UUID
    cluster_run_id: UUID | None
    label_run_id: UUID | None
    ground_truth_version: str
    evaluation_version: str
    label_metrics: list[LabelEvaluation]
    clustering_metrics: list[ClusteringEvaluation]
    coverage_metrics: list[DriftMetric]
    drift_metrics: list[DriftMetric]
    alerts: list[DriftAlert]
    snapshot: SnapshotMetadata
