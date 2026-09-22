"""ClickHouse reads and writes for immutable labeling snapshots."""

from datetime import UTC, datetime
from uuid import UUID

from clickhouse_connect.driver.client import Client

from chainlens.labels.models import ClusterMember, LabelRun, LabelSnapshot


class ClickHouseLabelStore:
    OUTPUT_TABLES = (
        "entities",
        "entity_members",
        "label_assignments",
        "label_evidence",
        "label_conflicts",
    )

    def __init__(self, client: Client) -> None:
        self.client = client

    def read_taxonomy(self, version: str) -> dict[str, set[str]]:
        rows = self.client.query(
            "SELECT label_dimension, label_value FROM chainlens.label_taxonomy "
            "WHERE taxonomy_version = {version:String} AND active = 1 GROUP BY "
            "label_dimension, label_value ORDER BY label_dimension, label_value",
            parameters={"version": version},
        ).result_rows
        taxonomy: dict[str, set[str]] = {}
        for dimension, value in rows:
            taxonomy.setdefault(dimension, set()).add(value)
        return taxonomy

    def read_cluster_members(
        self, chain_id: int, cluster_run_id: UUID | None
    ) -> list[ClusterMember]:
        if cluster_run_id is None:
            return []
        status_rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2) FROM ("
            "SELECT argMax(tuple(chain_id, status), version) AS state, max(version) AS max_version "
            "FROM chainlens.cluster_runs WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            parameters={"run_id": str(cluster_run_id)},
        ).result_rows
        if not status_rows:
            raise ValueError(f"cluster run {cluster_run_id} does not exist")
        run_chain_id, status = status_rows[0]
        if run_chain_id != chain_id or status != "completed":
            raise ValueError("cluster run must be completed and match label chain_id")
        rows = self.client.query(
            "SELECT chain_id, cluster_id, wallet_address, confidence, cluster_run_id "
            "FROM chainlens.wallet_clusters WHERE chain_id = {chain_id:UInt64} "
            "AND cluster_run_id = {run_id:UUID} ORDER BY cluster_id, wallet_address",
            parameters={"chain_id": chain_id, "run_id": str(cluster_run_id)},
        ).result_rows
        return [ClusterMember(*row) for row in rows]

    def get_run(self, run_id: UUID) -> LabelRun | None:
        rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), tupleElement(state, 3), "
            "tupleElement(state, 4), tupleElement(state, 5), tupleElement(state, 6), "
            "max_version FROM (SELECT argMax(tuple(chain_id, cluster_run_id, "
            "taxonomy_version, seed_dataset_version, status, started_at), version) AS state, "
            "max(version) AS max_version FROM chainlens.label_runs "
            "WHERE run_id = {run_id:UUID}) WHERE max_version > 0",
            parameters={"run_id": str(run_id)},
        ).result_rows
        if not rows:
            return None
        row = rows[0]
        return LabelRun(run_id, row[0], row[1], row[2], row[3], row[4], row[5], row[6])

    def write_run_state(
        self,
        run_id: UUID,
        chain_id: int,
        cluster_run_id: UUID | None,
        taxonomy_version: str,
        dataset_version: str,
        status: str,
        started_at: datetime,
        version: int,
        *,
        seed_count: int = 0,
        direct_label_count: int = 0,
        propagated_label_count: int = 0,
        entity_count: int = 0,
        error: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        self.client.insert(
            "chainlens.label_runs",
            [
                (
                    run_id,
                    chain_id,
                    cluster_run_id,
                    taxonomy_version,
                    dataset_version,
                    status,
                    started_at,
                    now if status in {"completed", "failed"} else None,
                    seed_count,
                    direct_label_count,
                    propagated_label_count,
                    entity_count,
                    error,
                    now,
                    version,
                )
            ],
            column_names=[
                "run_id", "chain_id", "cluster_run_id", "taxonomy_version",
                "seed_dataset_version", "status", "started_at", "completed_at",
                "seed_count", "direct_label_count", "propagated_label_count",
                "entity_count", "error", "updated_at", "version",
            ],
        )

    def write_snapshot(
        self, run_id: UUID, snapshot: LabelSnapshot, derived_at: datetime
    ) -> None:
        if snapshot.entities:
            self.client.insert(
                "chainlens.entities",
                [
                    (
                        item.chain_id, item.entity_id, item.display_name,
                        item.entity_category, item.created_from, item.cluster_id,
                        item.cluster_run_id, run_id, derived_at,
                    )
                    for item in snapshot.entities
                ],
                column_names=[
                    "chain_id", "entity_id", "display_name", "entity_category",
                    "created_from", "cluster_id", "cluster_run_id", "label_run_id",
                    "derived_at",
                ],
            )
        if snapshot.members:
            self.client.insert(
                "chainlens.entity_members",
                [
                    (
                        item.chain_id, item.entity_id, item.wallet_address,
                        item.membership_method, item.membership_confidence,
                        item.cluster_id, item.cluster_run_id, run_id, derived_at,
                    )
                    for item in snapshot.members
                ],
                column_names=[
                    "chain_id", "entity_id", "wallet_address", "membership_method",
                    "membership_confidence", "cluster_id", "cluster_run_id",
                    "label_run_id", "derived_at",
                ],
            )
        if snapshot.assignments:
            self.client.insert(
                "chainlens.label_assignments",
                [
                    (
                        item.chain_id, item.subject_type, item.subject_id, item.entity_id,
                        item.label_dimension, item.label_value, item.confidence,
                        item.assignment_method, item.source_type, item.source_reference,
                        run_id, item.taxonomy_version, derived_at,
                    )
                    for item in snapshot.assignments
                ],
                column_names=[
                    "chain_id", "subject_type", "subject_id", "entity_id",
                    "label_dimension", "label_value", "confidence",
                    "assignment_method", "source_type", "source_reference",
                    "label_run_id", "taxonomy_version", "derived_at",
                ],
            )
        if snapshot.evidence:
            self.client.insert(
                "chainlens.label_evidence",
                [
                    (
                        item.chain_id, item.subject_type, item.subject_id,
                        item.label_dimension, item.label_value, item.evidence_type,
                        item.evidence_value, item.weight, item.confidence_contribution,
                        item.source_type, item.source_reference, run_id, derived_at,
                    )
                    for item in snapshot.evidence
                ],
                column_names=[
                    "chain_id", "subject_type", "subject_id", "label_dimension",
                    "label_value", "evidence_type", "evidence_value", "weight",
                    "confidence_contribution", "source_type", "source_reference",
                    "label_run_id", "derived_at",
                ],
            )
        if snapshot.conflicts:
            self.client.insert(
                "chainlens.label_conflicts",
                [
                    (
                        item.chain_id, item.cluster_id, item.cluster_run_id,
                        item.conflict_type, item.details, run_id, derived_at,
                    )
                    for item in snapshot.conflicts
                ],
                column_names=[
                    "chain_id", "cluster_id", "cluster_run_id", "conflict_type",
                    "details", "label_run_id", "detected_at",
                ],
            )

    def delete_partial_run_outputs(self, run_id: UUID) -> None:
        for table in self.OUTPUT_TABLES:
            self.client.command(
                f"ALTER TABLE chainlens.{table} DELETE WHERE label_run_id = {{run_id:UUID}} "
                "SETTINGS mutations_sync = 1",
                parameters={"run_id": str(run_id)},
            )
