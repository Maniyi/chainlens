"""Small immutable models for label derivation and persistence."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class SeedLabel:
    chain_id: int
    address: str
    entity_name: str
    entity_category: str
    contract_role: str | None
    source_type: str
    source_reference: str
    confidence: float
    notes: str


@dataclass(frozen=True)
class ClusterMember:
    chain_id: int
    cluster_id: str
    wallet_address: str
    confidence: float
    cluster_run_id: UUID


@dataclass(frozen=True)
class Entity:
    chain_id: int
    entity_id: str
    display_name: str | None
    entity_category: str
    created_from: str
    cluster_id: str | None
    cluster_run_id: UUID | None


@dataclass(frozen=True)
class EntityMember:
    chain_id: int
    entity_id: str
    wallet_address: str
    membership_method: str
    membership_confidence: float
    cluster_id: str | None
    cluster_run_id: UUID | None


@dataclass(frozen=True)
class LabelAssignment:
    chain_id: int
    subject_type: str
    subject_id: str
    entity_id: str | None
    label_dimension: str
    label_value: str
    confidence: float
    assignment_method: str
    source_type: str
    source_reference: str
    taxonomy_version: str


@dataclass(frozen=True)
class LabelEvidence:
    chain_id: int
    subject_type: str
    subject_id: str
    label_dimension: str
    label_value: str
    evidence_type: str
    evidence_value: str
    weight: float
    confidence_contribution: float
    source_type: str
    source_reference: str


@dataclass(frozen=True)
class LabelConflict:
    chain_id: int
    cluster_id: str
    cluster_run_id: UUID
    conflict_type: str
    details: str


@dataclass(frozen=True)
class LabelSnapshot:
    entities: tuple[Entity, ...]
    members: tuple[EntityMember, ...]
    assignments: tuple[LabelAssignment, ...]
    evidence: tuple[LabelEvidence, ...]
    conflicts: tuple[LabelConflict, ...]


@dataclass(frozen=True)
class LabelRun:
    run_id: UUID
    chain_id: int
    cluster_run_id: UUID | None
    taxonomy_version: str
    seed_dataset_version: str
    status: str
    started_at: datetime
    version: int


@dataclass(frozen=True)
class LabelRunSummary:
    run_id: str
    chain_id: int
    cluster_run_id: str | None
    taxonomy_version: str
    seed_dataset_version: str
    seed_count: int
    direct_label_count: int
    propagated_label_count: int
    entity_count: int
    conflict_count: int
