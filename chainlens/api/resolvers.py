"""Application-level composition for GraphQL queries."""

import re
from typing import Any
from uuid import UUID

from chainlens.api.store import ClickHouseAPIStore
from chainlens.api.types import (
    Cluster, ClusterEvidence, ClusterMember, ClusteringEvaluation, DriftAlert,
    DriftMetric, Entity, EntityMember, EvaluationSummary, LabelAssignment,
    LabelEvaluation, LabelEvidence, ResolutionEdge, SnapshotMetadata, Wallet,
    WalletFeatures,
)

DEFAULT_LIMIT = 50
MAX_LIMIT = 500
MAX_OFFSET = 100_000
_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


class APIError(ValueError):
    """Safe, user-facing API input or snapshot error."""


def normalize_address(address: str) -> str:
    if not _EVM_ADDRESS.fullmatch(address):
        raise APIError("address must be 0x-prefixed followed by exactly 40 hex characters")
    return address.lower()


def pagination(limit: int = DEFAULT_LIMIT, offset: int = 0) -> tuple[int, int]:
    if limit < 1 or limit > MAX_LIMIT:
        raise APIError(f"limit must be between 1 and {MAX_LIMIT}")
    if offset < 0 or offset > MAX_OFFSET:
        raise APIError(f"offset must be between 0 and {MAX_OFFSET}")
    return limit, offset


class APIResolver:
    def __init__(self, store: ClickHouseAPIStore) -> None:
        self.store = store

    def _cluster_snapshot(self, chain_id: int, run_id: UUID | None) -> tuple[UUID | None, dict[str, Any] | None, bool]:
        if run_id is None:
            row = self.store.current_cluster_run(chain_id)
            return (row["run_id"], row, True) if row else (None, None, True)
        row = self.store.cluster_run(run_id)
        if row is None:
            raise APIError(f"cluster run {run_id} does not exist")
        if row["chain_id"] != chain_id:
            raise APIError(f"cluster run {run_id} does not belong to chain {chain_id}")
        if row["status"] != "completed":
            raise APIError(f"cluster run {run_id} is not completed")
        return run_id, row, False

    def _label_snapshot(self, chain_id: int, run_id: UUID | None) -> tuple[UUID | None, dict[str, Any] | None, bool]:
        if run_id is None:
            row = self.store.current_label_run(chain_id)
            return (row["run_id"], row, True) if row else (None, None, True)
        row = self.store.label_run(run_id)
        if row is None:
            raise APIError(f"label run {run_id} does not exist")
        if row["chain_id"] != chain_id:
            raise APIError(f"label run {run_id} does not belong to chain {chain_id}")
        if row["status"] != "completed":
            raise APIError(f"label run {run_id} is not completed")
        return run_id, row, False

    def _evaluation_snapshot(self, chain_id: int, run_id: UUID | None) -> tuple[UUID | None, dict[str, Any] | None, bool]:
        if run_id is None:
            row = self.store.current_evaluation_run(chain_id)
            return (row["run_id"], row, True) if row else (None, None, True)
        row = self.store.evaluation_run(run_id)
        if row is None:
            raise APIError(f"evaluation run {run_id} does not exist")
        if row["chain_id"] != chain_id:
            raise APIError(f"evaluation run {run_id} does not belong to chain {chain_id}")
        if row["status"] != "completed":
            raise APIError(f"evaluation run {run_id} is not completed")
        return run_id, row, False

    @staticmethod
    def _metadata(cluster_id: UUID | None, cluster: dict[str, Any] | None,
                  label_id: UUID | None = None, label: dict[str, Any] | None = None,
                  evaluation_id: UUID | None = None,
                  evaluation: dict[str, Any] | None = None) -> SnapshotMetadata:
        return SnapshotMetadata(
            cluster_run_id=cluster_id,
            label_run_id=label_id,
            evaluation_run_id=evaluation_id,
            heuristic_version=(evaluation or cluster or {}).get("heuristic_version"),
            taxonomy_version=(evaluation or label or {}).get("taxonomy_version"),
            ground_truth_version=(evaluation or {}).get("ground_truth_version"),
            evaluation_version=(evaluation or {}).get("evaluation_version"),
        )

    def _labels(self, chain_id: int, subject_type: str, subject_id: str,
                run_id: UUID | None, current: bool) -> list[LabelAssignment]:
        if run_id is None:
            return []
        evidence_rows = self.store.label_evidence(
            chain_id, subject_type, subject_id, run_id, current=current
        )
        result: list[LabelAssignment] = []
        for row in self.store.labels(chain_id, subject_type, subject_id, run_id, current=current):
            evidence = [
                LabelEvidence(
                    type=item["evidence_type"], value=item["evidence_value"],
                    weight=float(item["weight"]),
                    confidence_contribution=float(item["confidence_contribution"]),
                    source_type=item["source_type"], source_reference=item["source_reference"],
                )
                for item in evidence_rows
                if item["label_dimension"] == row["label_dimension"]
                and item["label_value"] == row["label_value"]
            ]
            result.append(LabelAssignment(
                dimension=row["label_dimension"], value=row["label_value"],
                confidence=float(row["confidence"]), assignment_method=row["assignment_method"],
                source_type=row["source_type"], source_reference=row["source_reference"],
                taxonomy_version=row["taxonomy_version"], evidence=evidence,
            ))
        return result

    def _entity_type(self, row: dict[str, Any], chain_id: int, run_id: UUID,
                     run: dict[str, Any], current: bool,
                     *, include_membership: bool = False) -> Entity:
        entity_id = row["entity_id"]

        def load_members(limit: int, offset: int) -> list[EntityMember]:
            return [EntityMember(
                address=item["wallet_address"], membership_method=item["membership_method"],
                membership_confidence=float(item["membership_confidence"]),
                cluster_id=item["cluster_id"],
            ) for item in self.store.entity_members(
                chain_id, entity_id, run_id, current=current, limit=limit, offset=offset
            )]

        assignments = self._labels(chain_id, "entity", entity_id, run_id, current)
        evidence = [e for assignment in assignments for e in assignment.evidence]
        return Entity(
            id=entity_id, display_name=row["display_name"], category=row["entity_category"],
            membership_method=row.get("membership_method") if include_membership else None,
            membership_confidence=(float(row["membership_confidence"])
                                   if include_membership else None),
            labels=assignments, evidence=evidence, cluster_id=row["cluster_id"],
            snapshot=self._metadata(row.get("cluster_run_id") or run.get("cluster_run_id"),
                                    None, run_id, run),
            _members_loader=load_members,
        )

    def _cluster_type(self, row: dict[str, Any], chain_id: int, run_id: UUID,
                      run: dict[str, Any], current: bool) -> Cluster:
        cluster_id = row["cluster_id"]

        def members(limit: int, offset: int) -> list[ClusterMember]:
            return [ClusterMember(address=item["wallet_address"], confidence=float(item["confidence"]))
                    for item in self.store.cluster_members(
                        chain_id, cluster_id, run_id, current=current, limit=limit, offset=offset)]

        def edges(limit: int, offset: int) -> list[ResolutionEdge]:
            return [ResolutionEdge(
                wallet_a=item["wallet_a"], wallet_b=item["wallet_b"],
                heuristic=item["heuristic"], score=float(item["score"]),
                evidence_count=int(item["evidence_count"]),
            ) for item in self.store.cluster_edges(
                chain_id, cluster_id, run_id, current=current, limit=limit, offset=offset)]

        def evidence(limit: int, offset: int) -> list[ClusterEvidence]:
            return [ClusterEvidence(
                wallet_a=item["wallet_a"], wallet_b=item["wallet_b"],
                heuristic=item["heuristic"], evidence_type=item["evidence_type"],
                evidence_value=item["evidence_value"], weight=float(item["weight"]),
                score_contribution=float(item["score_contribution"]),
            ) for item in self.store.cluster_evidence(
                chain_id, cluster_id, run_id, current=current, limit=limit, offset=offset)]

        return Cluster(
            id=cluster_id, size=int(row["cluster_size"]), confidence=float(row["confidence"]),
            heuristic_version=row["heuristic_version"],
            snapshot=self._metadata(run_id, run), _members_loader=members,
            _edges_loader=edges, _evidence_loader=evidence,
        )

    def wallet(self, address: str, chain_id: int = 8453,
               cluster_run_id: UUID | None = None,
               label_run_id: UUID | None = None) -> Wallet:
        address = normalize_address(address)
        cluster_id, cluster_run, cluster_current = self._cluster_snapshot(chain_id, cluster_run_id)
        label_id, label_run, label_current = self._label_snapshot(chain_id, label_run_id)
        if cluster_run_id is not None and label_run_id is not None:
            linked = label_run.get("cluster_run_id") if label_run else None
            if linked is not None and linked != cluster_run_id:
                raise APIError("label run is incompatible with the selected cluster run")

        features = None
        cluster = None
        if cluster_id is not None and cluster_run is not None:
            feature_row = self.store.wallet_features(
                chain_id, address, cluster_id, current=cluster_current
            )
            if feature_row:
                features = WalletFeatures(**feature_row)
            cluster_row = self.store.wallet_cluster(
                chain_id, address, cluster_id, current=cluster_current
            )
            if cluster_row:
                cluster = self._cluster_type(
                    cluster_row, chain_id, cluster_id, cluster_run, cluster_current
                )

        entity = None
        labels: list[LabelAssignment] = []
        if label_id is not None and label_run is not None:
            entity_row = self.store.entity_for_wallet(
                chain_id, address, label_id, current=label_current
            )
            if entity_row:
                entity = self._entity_type(
                    entity_row, chain_id, label_id, label_run, label_current,
                    include_membership=True,
                )
            labels = self._labels(chain_id, "address", address, label_id, label_current)
        return Wallet(
            address=address, chain_id=chain_id, features=features, entity=entity,
            cluster=cluster, labels=labels,
            snapshot=self._metadata(cluster_id, cluster_run, label_id, label_run),
        )

    def entity(self, entity_id: str, chain_id: int = 8453,
               label_run_id: UUID | None = None) -> Entity | None:
        run_id, run, current = self._label_snapshot(chain_id, label_run_id)
        if run_id is None or run is None:
            return None
        row = self.store.entity(chain_id, entity_id, run_id, current=current)
        return self._entity_type(row, chain_id, run_id, run, current) if row else None

    def entities(self, chain_id: int = 8453, category: str | None = None,
                 limit: int = DEFAULT_LIMIT, offset: int = 0,
                 label_run_id: UUID | None = None) -> list[Entity]:
        limit, offset = pagination(limit, offset)
        run_id, run, current = self._label_snapshot(chain_id, label_run_id)
        if run_id is None or run is None:
            return []
        return [self._entity_type(row, chain_id, run_id, run, current)
                for row in self.store.entities(
                    chain_id, run_id, current=current, category=category,
                    limit=limit, offset=offset)]

    def cluster(self, cluster_id: str, chain_id: int = 8453,
                cluster_run_id: UUID | None = None) -> Cluster | None:
        run_id, run, current = self._cluster_snapshot(chain_id, cluster_run_id)
        if run_id is None or run is None:
            return None
        row = self.store.cluster(chain_id, cluster_id, run_id, current=current)
        return self._cluster_type(row, chain_id, run_id, run, current) if row else None

    def clusters(self, chain_id: int = 8453, min_size: int | None = None,
                 limit: int = DEFAULT_LIMIT, offset: int = 0,
                 cluster_run_id: UUID | None = None) -> list[Cluster]:
        limit, offset = pagination(limit, offset)
        if min_size is not None and min_size < 1:
            raise APIError("minSize must be at least 1")
        run_id, run, current = self._cluster_snapshot(chain_id, cluster_run_id)
        if run_id is None or run is None:
            return []
        return [self._cluster_type(row, chain_id, run_id, run, current)
                for row in self.store.clusters(
                    chain_id, run_id, current=current, min_size=min_size,
                    limit=limit, offset=offset)]

    def evaluation(self, chain_id: int = 8453,
                   evaluation_run_id: UUID | None = None) -> EvaluationSummary | None:
        run_id, run, current = self._evaluation_snapshot(chain_id, evaluation_run_id)
        if run_id is None or run is None:
            return None
        label_metrics = [LabelEvaluation(**row) for row in
                         self.store.label_evaluations(chain_id, run_id, current=current)]
        cluster_metrics = [ClusteringEvaluation(**row) for row in
                           self.store.clustering_evaluations(chain_id, run_id, current=current)]
        drift = [DriftMetric(**row) for row in
                 self.store.drift_metrics(chain_id, run_id, current=current)]
        alerts = [DriftAlert(**row) for row in self.store.drift_alerts(
            chain_id, run_id, current=current, severity=None, limit=MAX_LIMIT)]
        metadata = self._metadata(run.get("cluster_run_id"), run,
                                  run.get("label_run_id"), run, run_id, run)
        return EvaluationSummary(
            run_id=run_id, cluster_run_id=run.get("cluster_run_id"),
            label_run_id=run.get("label_run_id"),
            ground_truth_version=run["ground_truth_version"],
            evaluation_version=run["evaluation_version"], label_metrics=label_metrics,
            clustering_metrics=cluster_metrics,
            coverage_metrics=[metric for metric in drift if metric.metric_group == "coverage"],
            drift_metrics=drift, alerts=alerts, snapshot=metadata,
        )

    def drift_alerts(self, chain_id: int = 8453, severity: str | None = None,
                     limit: int = DEFAULT_LIMIT,
                     evaluation_run_id: UUID | None = None) -> list[DriftAlert]:
        limit, _ = pagination(limit, 0)
        if severity is not None and severity not in {"info", "warning", "critical"}:
            raise APIError("severity must be one of: info, warning, critical")
        run_id, run, current = self._evaluation_snapshot(chain_id, evaluation_run_id)
        if run_id is None or run is None:
            return []
        return [DriftAlert(**row) for row in self.store.drift_alerts(
            chain_id, run_id, current=current, severity=severity, limit=limit)]
