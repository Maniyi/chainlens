"""Deterministic direct labeling, conflict detection, and safe propagation."""

import json
from collections import defaultdict
from collections.abc import Iterable
from hashlib import sha256

from chainlens.labels.models import (
    ClusterMember,
    Entity,
    EntityMember,
    LabelAssignment,
    LabelConflict,
    LabelEvidence,
    LabelSnapshot,
    SeedLabel,
)


def _stable_id(*parts: object) -> str:
    digest = sha256()
    for part in parts:
        encoded = str(part).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def normalize_entity_name(name: str) -> str:
    return " ".join(name.split()).casefold()


def seed_entity_id(chain_id: int, entity_name: str) -> str:
    """Stable logical ID from chain and normalized curated entity name."""

    return "seed_" + _stable_id(chain_id, normalize_entity_name(entity_name))


def cluster_entity_id(chain_id: int, cluster_id: str) -> str:
    return "cluster_" + _stable_id(chain_id, cluster_id)


def propagation_confidence(
    seed_confidence: float, membership_confidence: float, propagation_factor: float
) -> float:
    if not all(0.0 <= value <= 1.0 for value in (seed_confidence, membership_confidence, propagation_factor)):
        raise ValueError("confidence components must be in [0,1]")
    return min(1.0, max(0.0, seed_confidence * membership_confidence * propagation_factor))


def _assignment(
    seed: SeedLabel,
    *,
    subject_type: str,
    subject_id: str,
    entity_id: str | None,
    dimension: str,
    value: str,
    taxonomy_version: str,
) -> LabelAssignment:
    return LabelAssignment(
        seed.chain_id,
        subject_type,
        subject_id,
        entity_id,
        dimension,
        value,
        seed.confidence,
        "direct_seed",
        seed.source_type,
        seed.source_reference,
        taxonomy_version,
    )


def _direct_evidence(
    seed: SeedLabel, subject_type: str, subject_id: str, dimension: str, value: str
) -> LabelEvidence:
    return LabelEvidence(
        seed.chain_id,
        subject_type,
        subject_id,
        dimension,
        value,
        "curated_seed",
        seed.notes or f"curated label for {seed.address}",
        1.0,
        seed.confidence,
        seed.source_type,
        seed.source_reference,
    )


def derive_label_snapshot(
    chain_id: int,
    seeds: Iterable[SeedLabel],
    cluster_members: Iterable[ClusterMember],
    *,
    taxonomy_version: str,
    propagation_factor: float,
) -> LabelSnapshot:
    """Build one immutable logical snapshot without using interaction edges."""

    seed_list = sorted(
        (seed for seed in seeds if seed.chain_id == chain_id), key=lambda item: item.address
    )
    cluster_list = sorted(
        (member for member in cluster_members if member.chain_id == chain_id),
        key=lambda item: (item.cluster_id, item.wallet_address),
    )
    seeds_by_address = {seed.address: seed for seed in seed_list}
    seeds_by_entity: dict[str, list[SeedLabel]] = defaultdict(list)
    for seed in seed_list:
        seeds_by_entity[normalize_entity_name(seed.entity_name)].append(seed)
    for normalized_name, grouped in seeds_by_entity.items():
        if len({seed.entity_category for seed in grouped}) != 1:
            raise ValueError(f"conflicting categories for curated entity: {normalized_name}")

    entities: list[Entity] = []
    members: list[EntityMember] = []
    assignments: list[LabelAssignment] = []
    evidence: list[LabelEvidence] = []
    conflicts: list[LabelConflict] = []

    for normalized_name, grouped in sorted(seeds_by_entity.items()):
        exemplar = min(grouped, key=lambda item: item.address)
        entity_id = seed_entity_id(chain_id, exemplar.entity_name)
        entities.append(
            Entity(chain_id, entity_id, exemplar.entity_name, exemplar.entity_category, "direct_seed", None, None)
        )
        assignments.append(
            _assignment(
                exemplar,
                subject_type="entity",
                subject_id=entity_id,
                entity_id=entity_id,
                dimension="entity_category",
                value=exemplar.entity_category,
                taxonomy_version=taxonomy_version,
            )
        )
        evidence.append(
            _direct_evidence(exemplar, "entity", entity_id, "entity_category", exemplar.entity_category)
        )
        for seed in sorted(grouped, key=lambda item: item.address):
            members.append(EntityMember(chain_id, entity_id, seed.address, "direct_seed", seed.confidence, None, None))
            assignments.append(
                _assignment(
                    seed,
                    subject_type="address",
                    subject_id=seed.address,
                    entity_id=entity_id,
                    dimension="entity_category",
                    value=seed.entity_category,
                    taxonomy_version=taxonomy_version,
                )
            )
            evidence.append(_direct_evidence(seed, "address", seed.address, "entity_category", seed.entity_category))
            if seed.contract_role:
                assignments.append(
                    _assignment(
                        seed,
                        subject_type="address",
                        subject_id=seed.address,
                        entity_id=entity_id,
                        dimension="contract_role",
                        value=seed.contract_role,
                        taxonomy_version=taxonomy_version,
                    )
                )
                evidence.append(_direct_evidence(seed, "address", seed.address, "contract_role", seed.contract_role))

    by_cluster: dict[str, list[ClusterMember]] = defaultdict(list)
    for member in cluster_list:
        by_cluster[member.cluster_id].append(member)
    for cluster_id, grouped_members in sorted(by_cluster.items()):
        cluster_run_id = grouped_members[0].cluster_run_id
        if any(member.cluster_run_id != cluster_run_id for member in grouped_members):
            raise ValueError(f"cluster {cluster_id} mixes clustering snapshots")
        entity_id = cluster_entity_id(chain_id, cluster_id)
        direct_seeds = [
            seeds_by_address[member.wallet_address]
            for member in grouped_members
            if member.wallet_address in seeds_by_address
        ]
        identities = {normalize_entity_name(seed.entity_name) for seed in direct_seeds}
        ambiguous = len(identities) > 1
        selected_seed: SeedLabel | None = None
        if ambiguous:
            details = json.dumps(
                {
                    "addresses": sorted(seed.address for seed in direct_seeds),
                    "entity_names": sorted({seed.entity_name for seed in direct_seeds}),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            conflicts.append(
                LabelConflict(chain_id, cluster_id, cluster_run_id, "conflicting_direct_seed_identities", details)
            )
        elif direct_seeds:
            selected_seed = min(direct_seeds, key=lambda seed: (-seed.confidence, seed.address))
        entities.append(
            Entity(
                chain_id,
                entity_id,
                selected_seed.entity_name if selected_seed else None,
                selected_seed.entity_category if selected_seed else "unknown",
                "cluster_membership",
                cluster_id,
                cluster_run_id,
            )
        )
        for member in grouped_members:
            members.append(
                EntityMember(
                    chain_id,
                    entity_id,
                    member.wallet_address,
                    "cluster_membership",
                    member.confidence,
                    cluster_id,
                    cluster_run_id,
                )
            )
        if selected_seed is None:
            continue

        source_member = next(
            member for member in grouped_members if member.wallet_address == selected_seed.address
        )
        targets: list[tuple[str, str, float]] = [
            ("entity", entity_id, source_member.confidence)
        ]
        targets.extend(
            ("address", member.wallet_address, member.confidence)
            for member in grouped_members
            if member.wallet_address not in seeds_by_address
        )
        for subject_type, subject_id, membership_confidence in targets:
            confidence = propagation_confidence(
                selected_seed.confidence, membership_confidence, propagation_factor
            )
            assignments.append(
                LabelAssignment(
                    chain_id,
                    subject_type,
                    subject_id,
                    entity_id,
                    "entity_category",
                    selected_seed.entity_category,
                    confidence,
                    "cluster_propagation",
                    selected_seed.source_type,
                    selected_seed.source_reference,
                    taxonomy_version,
                )
            )
            for evidence_type, evidence_value, weight, contribution in (
                ("labeled_cluster_member", selected_seed.address, selected_seed.confidence, selected_seed.confidence),
                ("cluster_membership_confidence", format(membership_confidence, ".17g"), membership_confidence, membership_confidence),
                ("propagation_factor", format(propagation_factor, ".17g"), propagation_factor, propagation_factor),
            ):
                evidence.append(
                    LabelEvidence(
                        chain_id,
                        subject_type,
                        subject_id,
                        "entity_category",
                        selected_seed.entity_category,
                        evidence_type,
                        evidence_value,
                        weight,
                        contribution,
                        selected_seed.source_type,
                        selected_seed.source_reference,
                    )
                )

    return LabelSnapshot(
        tuple(sorted(entities, key=lambda item: item.entity_id)),
        tuple(sorted(members, key=lambda item: (item.entity_id, item.wallet_address, item.membership_method))),
        tuple(sorted(assignments, key=lambda item: (item.subject_type, item.subject_id, item.label_dimension, item.assignment_method, item.source_reference))),
        tuple(sorted(evidence, key=lambda item: (item.subject_type, item.subject_id, item.label_dimension, item.evidence_type, item.evidence_value))),
        tuple(sorted(conflicts, key=lambda item: (item.cluster_id, item.conflict_type))),
    )
