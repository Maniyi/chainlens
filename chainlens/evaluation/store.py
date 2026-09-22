"""ClickHouse persistence for immutable evaluation snapshots."""

from datetime import UTC, datetime
from uuid import UUID

from clickhouse_connect.driver.client import Client

from chainlens.evaluation.models import (
    ClusterEvaluation,
    DriftAlert,
    DriftMetric,
    EvaluatedLabel,
    EvaluationRun,
    SnapshotScope,
    StoredLabelAssignment,
)


class ClickHouseEvaluationStore:
    def __init__(self, client: Client) -> None:
        self.client = client

    def read_scope(
        self, chain_id: int, cluster_run_id: UUID, label_run_id: UUID
    ) -> SnapshotScope:
        cluster_rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3), tupleElement(state, 4), tupleElement(state, 5), "
            "tupleElement(state, 6), tupleElement(state, 7) FROM (SELECT argMax(tuple("
            "chain_id, start_block, end_block, heuristic_version, status, wallet_count, "
            "resolution_edge_count), version) AS state, max(version) AS max_version "
            "FROM chainlens.cluster_runs WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            parameters={"run_id": str(cluster_run_id)},
        ).result_rows
        if not cluster_rows:
            raise ValueError(f"cluster run {cluster_run_id} does not exist")
        cluster = cluster_rows[0]
        if cluster[0] != chain_id or cluster[4] != "completed":
            raise ValueError("cluster run must be completed and match evaluation chain_id")

        label_rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3), tupleElement(state, 4), tupleElement(state, 5), "
            "tupleElement(state, 6), tupleElement(state, 7), tupleElement(state, 8) "
            "FROM (SELECT argMax(tuple(chain_id, cluster_run_id, taxonomy_version, "
            "seed_dataset_version, status, entity_count, direct_label_count, "
            "propagated_label_count), version) AS state, max(version) AS max_version "
            "FROM chainlens.label_runs WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            parameters={"run_id": str(label_run_id)},
        ).result_rows
        if not label_rows:
            raise ValueError(f"label run {label_run_id} does not exist")
        label = label_rows[0]
        if label[0] != chain_id or label[4] != "completed":
            raise ValueError("label run must be completed and match evaluation chain_id")
        if label[1] != cluster_run_id:
            raise ValueError("label run must use the selected cluster snapshot")

        cluster_count = self.client.query(
            "SELECT uniqExact(cluster_id) FROM chainlens.wallet_clusters "
            "WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID}",
            parameters={"chain_id": chain_id, "run_id": str(cluster_run_id)},
        ).first_row[0]
        return SnapshotScope(
            chain_id,
            cluster_run_id,
            label_run_id,
            cluster[1],
            cluster[2],
            cluster[3],
            label[2],
            label[3],
            cluster[5],
            cluster[6],
            cluster_count,
            label[5],
            label[6],
            label[7],
        )

    def read_seen_addresses(self, scope: SnapshotScope) -> set[str]:
        rows = self.client.query(
            "SELECT wallet_address FROM chainlens.wallet_features "
            "WHERE chain_id = {chain_id:UInt64} AND run_id = {run_id:UUID}",
            parameters={"chain_id": scope.chain_id, "run_id": str(scope.cluster_run_id)},
        ).result_rows
        return {row[0].lower() for row in rows}

    def read_clusters(
        self, scope: SnapshotScope
    ) -> tuple[dict[str, str], dict[str, float]]:
        rows = self.client.query(
            "SELECT wallet_address, cluster_id, confidence FROM chainlens.wallet_clusters "
            "WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID}",
            parameters={"chain_id": scope.chain_id, "run_id": str(scope.cluster_run_id)},
        ).result_rows
        return (
            {row[0].lower(): row[1] for row in rows},
            {row[0].lower(): float(row[2]) for row in rows},
        )

    def read_assignments(self, scope: SnapshotScope) -> list[StoredLabelAssignment]:
        rows = self.client.query(
            "SELECT subject_type, subject_id, entity_id, label_dimension, label_value, "
            "confidence, assignment_method FROM chainlens.label_assignments "
            "WHERE chain_id = {chain_id:UInt64} AND label_run_id = {run_id:UUID}",
            parameters={"chain_id": scope.chain_id, "run_id": str(scope.label_run_id)},
        ).result_rows
        return [StoredLabelAssignment(*row) for row in rows]

    def read_conflict_count(self, scope: SnapshotScope) -> int:
        return int(
            self.client.query(
                "SELECT count() FROM chainlens.label_conflicts "
                "WHERE chain_id = {chain_id:UInt64} AND label_run_id = {run_id:UUID}",
                parameters={"chain_id": scope.chain_id, "run_id": str(scope.label_run_id)},
            ).first_row[0]
        )

    def get_run(self, run_id: UUID) -> EvaluationRun | None:
        rows = self.client.query(
            "SELECT state, max_version FROM (SELECT argMax(tuple(chain_id, cluster_run_id, "
            "label_run_id, start_block, end_block, heuristic_version, taxonomy_version, "
            "seed_dataset_version, ground_truth_version, evaluation_version, status, "
            "started_at), version) AS state, max(version) AS max_version "
            "FROM chainlens.evaluation_runs WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            parameters={"run_id": str(run_id)},
        ).result_rows
        if not rows:
            return None
        values, version = rows[0]
        scope = SnapshotScope(
            values[0], values[1], values[2], values[3], values[4], values[5],
            values[6], values[7], 0, 0, 0, 0, 0, 0,
        )
        return EvaluationRun(run_id, scope, values[8], values[9], values[10], values[11], version)

    def write_run_state(
        self,
        run_id: UUID,
        scope: SnapshotScope,
        ground_truth_version: str,
        evaluation_version: str,
        status: str,
        started_at: datetime,
        version: int,
        *,
        error: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        self.client.insert(
            "chainlens.evaluation_runs",
            [(
                run_id, scope.chain_id, scope.cluster_run_id, scope.label_run_id,
                scope.start_block, scope.end_block, scope.heuristic_version,
                scope.taxonomy_version, scope.seed_dataset_version,
                ground_truth_version, evaluation_version, status, started_at,
                now if status in {"completed", "failed"} else None, error, now, version,
            )],
            column_names=[
                "run_id", "chain_id", "cluster_run_id", "label_run_id", "start_block",
                "end_block", "heuristic_version", "taxonomy_version",
                "seed_dataset_version", "ground_truth_version", "evaluation_version",
                "status", "started_at", "completed_at", "error", "updated_at", "version",
            ],
        )

    def read_previous_metrics(
        self,
        scope: SnapshotScope,
        ground_truth_version: str,
        evaluation_version: str,
    ) -> dict[tuple[str, str, str], float]:
        rows = self.client.query(
            "SELECT run_id FROM chainlens.latest_evaluation_runs WHERE "
            "chain_id = {chain_id:UInt64} AND start_block = {start:UInt64} "
            "AND end_block = {end:UInt64} AND heuristic_version = {heuristic:String} "
            "AND taxonomy_version = {taxonomy:String} "
            "AND ground_truth_version = {ground_truth:String} "
            "AND evaluation_version = {evaluation:String}",
            parameters={
                "chain_id": scope.chain_id, "start": scope.start_block, "end": scope.end_block,
                "heuristic": scope.heuristic_version, "taxonomy": scope.taxonomy_version,
                "ground_truth": ground_truth_version, "evaluation": evaluation_version,
            },
        ).result_rows
        if not rows:
            return {}
        metric_rows = self.client.query(
            "SELECT metric_group, metric_name, scope_key, value FROM chainlens.drift_metrics "
            "WHERE evaluation_run_id = {run_id:UUID}",
            parameters={"run_id": str(rows[0][0])},
        ).result_rows
        return {(row[0], row[1], row[2]): float(row[3]) for row in metric_rows}

    def write_results(
        self,
        run_id: UUID,
        scope: SnapshotScope,
        label_rows: list[EvaluatedLabel],
        cluster_row: ClusterEvaluation,
        drift_rows: list[DriftMetric],
        alerts: list[DriftAlert],
        evaluated_at: datetime,
    ) -> None:
        if label_rows:
            self.client.insert(
                "chainlens.label_evaluations",
                [(
                    run_id, scope.chain_id, row.label_dimension, row.label_value,
                    row.assignment_method, row.tp, row.fp, row.fn, row.precision,
                    row.recall, row.f1, row.support, row.predicted_count, row.coverage,
                    evaluated_at,
                ) for row in label_rows],
                column_names=[
                    "evaluation_run_id", "chain_id", "label_dimension", "label_value",
                    "assignment_method", "tp", "fp", "fn", "precision", "recall", "f1",
                    "support", "predicted_count", "coverage", "evaluated_at",
                ],
            )
        self.client.insert(
            "chainlens.clustering_evaluations",
            [(
                run_id, scope.chain_id, scope.cluster_run_id, cluster_row.metric_scope,
                cluster_row.tp_pairs, cluster_row.fp_pairs, cluster_row.fn_pairs,
                cluster_row.pairwise_precision, cluster_row.pairwise_recall,
                cluster_row.pairwise_f1, cluster_row.ground_truth_entity_count,
                cluster_row.ground_truth_address_count, cluster_row.evaluated_address_count,
                cluster_row.predicted_cluster_count, evaluated_at,
            )],
            column_names=[
                "evaluation_run_id", "chain_id", "cluster_run_id", "metric_scope",
                "tp_pairs", "fp_pairs", "fn_pairs", "pairwise_precision",
                "pairwise_recall", "pairwise_f1", "ground_truth_entity_count",
                "ground_truth_address_count", "evaluated_address_count",
                "predicted_cluster_count", "evaluated_at",
            ],
        )
        if drift_rows:
            self.client.insert(
                "chainlens.drift_metrics",
                [(
                    run_id, scope.chain_id, row.metric_group, row.metric_name,
                    row.scope_key, row.value, row.previous_value, row.absolute_change,
                    row.relative_change, evaluated_at,
                ) for row in drift_rows],
                column_names=[
                    "evaluation_run_id", "chain_id", "metric_group", "metric_name",
                    "scope_key", "value", "previous_value", "absolute_change",
                    "relative_change", "recorded_at",
                ],
            )
        if alerts:
            from uuid import uuid4

            self.client.insert(
                "chainlens.drift_alerts",
                [(
                    uuid4(), run_id, scope.chain_id, row.metric_name, row.scope_key,
                    row.severity, row.current_value, row.previous_value,
                    row.threshold_type, row.threshold_value, row.message, evaluated_at,
                ) for row in alerts],
                column_names=[
                    "alert_id", "evaluation_run_id", "chain_id", "metric_name",
                    "scope_key", "severity", "current_value", "previous_value",
                    "threshold_type", "threshold_value", "message", "created_at",
                ],
            )
