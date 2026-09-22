"""Bounded, parameterized ClickHouse reads for the API."""

from typing import Any, Iterable
from uuid import UUID

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError


class ClickHouseUnavailable(RuntimeError):
    """Safe API-facing replacement for driver/network details."""


class ClickHouseAPIStore:
    """Read existing current views and immutable historical snapshots only."""

    def __init__(self, client: Client) -> None:
        self.client = client

    def close(self) -> None:
        self.client.close()

    def ping(self) -> None:
        self.client.query("SELECT 1")

    def _rows(
        self, sql: str, columns: Iterable[str], parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        try:
            rows = self.client.query(sql, parameters=parameters or {}).result_rows
        except ClickHouseError as exc:
            raise ClickHouseUnavailable("ClickHouse unavailable") from exc
        names = tuple(columns)
        return [dict(zip(names, row, strict=True)) for row in rows]

    def current_cluster_run(self, chain_id: int) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT run_id, heuristic_version FROM chainlens.latest_cluster_runs "
            "WHERE chain_id = {chain_id:UInt64} "
            "ORDER BY completed_at DESC, toString(run_id) DESC LIMIT 1",
            ("run_id", "heuristic_version"), {"chain_id": chain_id},
        )
        return rows[0] if rows else None

    def cluster_run(self, run_id: UUID) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3) FROM (SELECT argMax(tuple(chain_id, status, "
            "heuristic_version), version) AS state, max(version) AS max_version "
            "FROM chainlens.cluster_runs WHERE run_id = {run_id:UUID}) "
            "WHERE max_version > 0",
            ("chain_id", "status", "heuristic_version"), {"run_id": str(run_id)},
        )
        return rows[0] if rows else None

    def current_label_run(self, chain_id: int) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT run_id, cluster_run_id, taxonomy_version "
            "FROM chainlens.latest_label_runs WHERE chain_id = {chain_id:UInt64} "
            "ORDER BY completed_at DESC, toString(run_id) DESC LIMIT 1",
            ("run_id", "cluster_run_id", "taxonomy_version"), {"chain_id": chain_id},
        )
        return rows[0] if rows else None

    def label_run(self, run_id: UUID) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3), tupleElement(state, 4) FROM (SELECT "
            "argMax(tuple(chain_id, status, cluster_run_id, taxonomy_version), "
            "version) AS state, max(version) AS max_version FROM chainlens.label_runs "
            "WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            ("chain_id", "status", "cluster_run_id", "taxonomy_version"),
            {"run_id": str(run_id)},
        )
        return rows[0] if rows else None

    def current_evaluation_run(self, chain_id: int) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT run_id, cluster_run_id, label_run_id, heuristic_version, "
            "taxonomy_version, ground_truth_version, evaluation_version "
            "FROM chainlens.latest_evaluation_runs WHERE chain_id = {chain_id:UInt64} "
            "ORDER BY completed_at DESC, toString(run_id) DESC LIMIT 1",
            ("run_id", "cluster_run_id", "label_run_id", "heuristic_version",
             "taxonomy_version", "ground_truth_version", "evaluation_version"),
            {"chain_id": chain_id},
        )
        return rows[0] if rows else None

    def evaluation_run(self, run_id: UUID) -> dict[str, Any] | None:
        columns = ("chain_id", "status", "cluster_run_id", "label_run_id",
                   "heuristic_version", "taxonomy_version", "ground_truth_version",
                   "evaluation_version")
        rows = self._rows(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), "
            "tupleElement(state, 3), tupleElement(state, 4), tupleElement(state, 5), "
            "tupleElement(state, 6), tupleElement(state, 7), tupleElement(state, 8) "
            "FROM (SELECT argMax(tuple(chain_id, status, cluster_run_id, label_run_id, "
            "heuristic_version, taxonomy_version, ground_truth_version, "
            "evaluation_version), version) AS state, max(version) AS max_version "
            "FROM chainlens.evaluation_runs WHERE run_id = {run_id:UUID}) "
            "WHERE max_version > 0",
            columns, {"run_id": str(run_id)},
        )
        return rows[0] if rows else None

    @staticmethod
    def _source(current: bool, current_name: str, history_name: str) -> str:
        return f"chainlens.{current_name if current else history_name}"

    def wallet_features(self, chain_id: int, address: str, run_id: UUID, *, current: bool) -> dict[str, Any] | None:
        source = self._source(current, "current_wallet_features", "wallet_features")
        rows = self._rows(
            "SELECT first_seen_block, last_seen_block, incoming_native_tx_count, "
            "outgoing_native_tx_count, incoming_erc20_transfer_count, "
            "outgoing_erc20_transfer_count, unique_counterparties, "
            "toString(native_received_wei), toString(native_sent_wei), "
            f"first_funder_address, first_funding_block FROM {source} "
            "WHERE chain_id = {chain_id:UInt64} AND wallet_address = {address:String} "
            "AND run_id = {run_id:UUID} LIMIT 1",
            ("first_seen_block", "last_seen_block", "incoming_native_tx_count",
             "outgoing_native_tx_count", "incoming_erc20_transfer_count",
             "outgoing_erc20_transfer_count", "unique_counterparties",
             "native_received_wei", "native_sent_wei", "first_funder_address",
             "first_funding_block"),
            {"chain_id": chain_id, "address": address, "run_id": str(run_id)},
        )
        return rows[0] if rows else None

    def wallet_cluster(self, chain_id: int, address: str, run_id: UUID, *, current: bool) -> dict[str, Any] | None:
        source = self._source(current, "current_wallet_clusters", "wallet_clusters")
        rows = self._rows(
            "SELECT cluster_id, cluster_size, confidence, heuristic_version "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND wallet_address = {address:String} AND cluster_run_id = {run_id:UUID} LIMIT 1",
            ("cluster_id", "cluster_size", "confidence", "heuristic_version"),
            {"chain_id": chain_id, "address": address, "run_id": str(run_id)},
        )
        return rows[0] if rows else None

    def entity_for_wallet(self, chain_id: int, address: str, run_id: UUID, *, current: bool) -> dict[str, Any] | None:
        members = self._source(current, "current_entity_members", "entity_members")
        entities = self._source(current, "current_entities", "entities")
        rows = self._rows(
            "SELECT e.entity_id, e.display_name, e.entity_category, e.cluster_id, "
            "m.membership_method, m.membership_confidence FROM " + members + " AS m "
            "INNER JOIN " + entities + " AS e ON m.chain_id = e.chain_id "
            "AND m.label_run_id = e.label_run_id AND m.entity_id = e.entity_id "
            "WHERE m.chain_id = {chain_id:UInt64} AND m.label_run_id = {run_id:UUID} "
            "AND m.wallet_address = {address:String} "
            "ORDER BY (m.membership_method = 'direct_seed') DESC, m.entity_id LIMIT 1",
            ("entity_id", "display_name", "entity_category", "cluster_id",
             "membership_method", "membership_confidence"),
            {"chain_id": chain_id, "run_id": str(run_id), "address": address},
        )
        return rows[0] if rows else None

    def entity(self, chain_id: int, entity_id: str, run_id: UUID, *, current: bool) -> dict[str, Any] | None:
        source = self._source(current, "current_entities", "entities")
        rows = self._rows(
            "SELECT entity_id, display_name, entity_category, cluster_id "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND label_run_id = {run_id:UUID} AND entity_id = {entity_id:String} LIMIT 1",
            ("entity_id", "display_name", "entity_category", "cluster_id"),
            {"chain_id": chain_id, "run_id": str(run_id), "entity_id": entity_id},
        )
        return rows[0] if rows else None

    def entity_members(self, chain_id: int, entity_id: str, run_id: UUID, *, current: bool, limit: int, offset: int) -> list[dict[str, Any]]:
        source = self._source(current, "current_entity_members", "entity_members")
        return self._rows(
            "SELECT wallet_address, membership_method, membership_confidence, cluster_id "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND label_run_id = {run_id:UUID} AND entity_id = {entity_id:String} "
            "ORDER BY wallet_address, membership_method LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("wallet_address", "membership_method", "membership_confidence", "cluster_id"),
            {"chain_id": chain_id, "run_id": str(run_id), "entity_id": entity_id,
             "limit": limit, "offset": offset},
        )

    def entities(self, chain_id: int, run_id: UUID, *, current: bool, category: str | None, limit: int, offset: int) -> list[dict[str, Any]]:
        source = self._source(current, "current_entities", "entities")
        category_sql = " AND entity_category = {category:String}" if category else ""
        return self._rows(
            "SELECT entity_id, display_name, entity_category, cluster_id "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND label_run_id = {run_id:UUID}" + category_sql +
            " ORDER BY entity_id LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("entity_id", "display_name", "entity_category", "cluster_id"),
            {"chain_id": chain_id, "run_id": str(run_id), "category": category or "",
             "limit": limit, "offset": offset},
        )

    def labels(self, chain_id: int, subject_type: str, subject_id: str, run_id: UUID, *, current: bool) -> list[dict[str, Any]]:
        source = self._source(current, "current_label_assignments", "label_assignments")
        return self._rows(
            "SELECT label_dimension, label_value, confidence, assignment_method, "
            "source_type, source_reference, taxonomy_version "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND label_run_id = {run_id:UUID} AND subject_type = {subject_type:String} "
            "AND subject_id = {subject_id:String} ORDER BY label_dimension, label_value, assignment_method",
            ("label_dimension", "label_value", "confidence", "assignment_method",
             "source_type", "source_reference", "taxonomy_version"),
            {"chain_id": chain_id, "run_id": str(run_id), "subject_type": subject_type,
             "subject_id": subject_id},
        )

    def label_evidence(self, chain_id: int, subject_type: str, subject_id: str, run_id: UUID, *, current: bool) -> list[dict[str, Any]]:
        source = self._source(current, "current_label_evidence", "label_evidence")
        return self._rows(
            "SELECT label_dimension, label_value, evidence_type, evidence_value, "
            "weight, confidence_contribution, source_type, source_reference "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND label_run_id = {run_id:UUID} AND subject_type = {subject_type:String} "
            "AND subject_id = {subject_id:String} ORDER BY label_dimension, label_value, evidence_type, evidence_value",
            ("label_dimension", "label_value", "evidence_type", "evidence_value", "weight",
             "confidence_contribution", "source_type", "source_reference"),
            {"chain_id": chain_id, "run_id": str(run_id), "subject_type": subject_type,
             "subject_id": subject_id},
        )

    def cluster(self, chain_id: int, cluster_id: str, run_id: UUID, *, current: bool) -> dict[str, Any] | None:
        source = self._source(current, "current_wallet_clusters", "wallet_clusters")
        rows = self._rows(
            "SELECT cluster_id, max(cluster_size), min(confidence), any(heuristic_version) "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND cluster_run_id = {run_id:UUID} AND cluster_id = {cluster_id:String} "
            "GROUP BY cluster_id LIMIT 1",
            ("cluster_id", "cluster_size", "confidence", "heuristic_version"),
            {"chain_id": chain_id, "run_id": str(run_id), "cluster_id": cluster_id},
        )
        return rows[0] if rows else None

    def cluster_members(self, chain_id: int, cluster_id: str, run_id: UUID, *, current: bool, limit: int, offset: int) -> list[dict[str, Any]]:
        source = self._source(current, "current_wallet_clusters", "wallet_clusters")
        return self._rows(
            "SELECT wallet_address, confidence " + f"FROM {source} "
            "WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID} "
            "AND cluster_id = {cluster_id:String} ORDER BY wallet_address "
            "LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("wallet_address", "confidence"),
            {"chain_id": chain_id, "run_id": str(run_id), "cluster_id": cluster_id,
             "limit": limit, "offset": offset},
        )

    def clusters(self, chain_id: int, run_id: UUID, *, current: bool, min_size: int | None, limit: int, offset: int) -> list[dict[str, Any]]:
        source = self._source(current, "current_wallet_clusters", "wallet_clusters")
        having = " HAVING cluster_size >= {min_size:UInt64}" if min_size is not None else ""
        return self._rows(
            "SELECT cluster_id, max(cluster_size) AS cluster_size, min(confidence), "
            f"any(heuristic_version) FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND cluster_run_id = {run_id:UUID} GROUP BY cluster_id" + having +
            " ORDER BY cluster_size DESC, cluster_id LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("cluster_id", "cluster_size", "confidence", "heuristic_version"),
            {"chain_id": chain_id, "run_id": str(run_id), "min_size": min_size or 0,
             "limit": limit, "offset": offset},
        )

    def cluster_edges(self, chain_id: int, cluster_id: str, run_id: UUID, *, current: bool, limit: int, offset: int) -> list[dict[str, Any]]:
        edges = self._source(current, "current_entity_resolution_edges", "entity_resolution_edges")
        clusters = self._source(current, "current_wallet_clusters", "wallet_clusters")
        return self._rows(
            "SELECT wallet_a, wallet_b, heuristic, score, evidence_count FROM " + edges +
            " WHERE chain_id = {chain_id:UInt64} AND run_id = {run_id:UUID} "
            "AND wallet_a IN (SELECT wallet_address FROM " + clusters +
            " WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID} AND cluster_id = {cluster_id:String}) "
            "AND wallet_b IN (SELECT wallet_address FROM " + clusters +
            " WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID} AND cluster_id = {cluster_id:String}) "
            "ORDER BY wallet_a, wallet_b, heuristic LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("wallet_a", "wallet_b", "heuristic", "score", "evidence_count"),
            {"chain_id": chain_id, "run_id": str(run_id), "cluster_id": cluster_id,
             "limit": limit, "offset": offset},
        )

    def cluster_evidence(self, chain_id: int, cluster_id: str, run_id: UUID, *, current: bool, limit: int, offset: int) -> list[dict[str, Any]]:
        evidence = self._source(current, "current_cluster_evidence", "cluster_evidence")
        clusters = self._source(current, "current_wallet_clusters", "wallet_clusters")
        return self._rows(
            "SELECT wallet_a, wallet_b, heuristic, evidence_type, evidence_value, weight, score_contribution FROM " + evidence +
            " WHERE chain_id = {chain_id:UInt64} AND run_id = {run_id:UUID} "
            "AND wallet_a IN (SELECT wallet_address FROM " + clusters +
            " WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID} AND cluster_id = {cluster_id:String}) "
            "AND wallet_b IN (SELECT wallet_address FROM " + clusters +
            " WHERE chain_id = {chain_id:UInt64} AND cluster_run_id = {run_id:UUID} AND cluster_id = {cluster_id:String}) "
            "ORDER BY wallet_a, wallet_b, heuristic, evidence_type LIMIT {limit:UInt64} OFFSET {offset:UInt64}",
            ("wallet_a", "wallet_b", "heuristic", "evidence_type", "evidence_value",
             "weight", "score_contribution"),
            {"chain_id": chain_id, "run_id": str(run_id), "cluster_id": cluster_id,
             "limit": limit, "offset": offset},
        )

    def label_evaluations(self, chain_id: int, run_id: UUID, *, current: bool) -> list[dict[str, Any]]:
        source = self._source(current, "current_label_evaluations", "label_evaluations")
        return self._rows(
            "SELECT label_dimension, label_value, assignment_method, tp, fp, fn, "
            "precision, recall, f1, support, predicted_count, coverage "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} AND evaluation_run_id = {{run_id:UUID}} "
            "ORDER BY assignment_method, label_dimension, label_value",
            ("label_dimension", "label_value", "assignment_method", "tp", "fp", "fn",
             "precision", "recall", "f1", "support", "predicted_count", "coverage"),
            {"chain_id": chain_id, "run_id": str(run_id)},
        )

    def clustering_evaluations(self, chain_id: int, run_id: UUID, *, current: bool) -> list[dict[str, Any]]:
        source = self._source(current, "current_clustering_evaluations", "clustering_evaluations")
        return self._rows(
            "SELECT metric_scope, tp_pairs, fp_pairs, fn_pairs, pairwise_precision, "
            "pairwise_recall, pairwise_f1, ground_truth_entity_count, "
            "ground_truth_address_count, evaluated_address_count, predicted_cluster_count "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} AND evaluation_run_id = {{run_id:UUID}} ORDER BY metric_scope",
            ("metric_scope", "tp_pairs", "fp_pairs", "fn_pairs", "pairwise_precision",
             "pairwise_recall", "pairwise_f1", "ground_truth_entity_count",
             "ground_truth_address_count", "evaluated_address_count", "predicted_cluster_count"),
            {"chain_id": chain_id, "run_id": str(run_id)},
        )

    def drift_metrics(self, chain_id: int, run_id: UUID, *, current: bool) -> list[dict[str, Any]]:
        source = self._source(current, "current_drift_metrics", "drift_metrics")
        return self._rows(
            "SELECT metric_group, metric_name, scope_key, value, previous_value, "
            f"absolute_change, relative_change FROM {source} WHERE chain_id = {{chain_id:UInt64}} "
            "AND evaluation_run_id = {run_id:UUID} ORDER BY metric_group, metric_name, scope_key",
            ("metric_group", "metric_name", "scope_key", "value", "previous_value",
             "absolute_change", "relative_change"),
            {"chain_id": chain_id, "run_id": str(run_id)},
        )

    def drift_alerts(self, chain_id: int, run_id: UUID, *, current: bool, severity: str | None, limit: int) -> list[dict[str, Any]]:
        source = self._source(current, "current_drift_alerts", "drift_alerts")
        severity_sql = " AND severity = {severity:String}" if severity else ""
        return self._rows(
            "SELECT severity, metric_name, scope_key, current_value, previous_value, "
            "threshold_type, threshold_value, message, created_at "
            f"FROM {source} WHERE chain_id = {{chain_id:UInt64}} AND evaluation_run_id = {{run_id:UUID}}" +
            severity_sql + " ORDER BY severity DESC, metric_name, scope_key LIMIT {limit:UInt64}",
            ("severity", "metric_name", "scope_key", "current_value", "previous_value",
             "threshold_type", "threshold_value", "message", "created_at"),
            {"chain_id": chain_id, "run_id": str(run_id), "severity": severity or "",
             "limit": limit},
        )
