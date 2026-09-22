"""Focused deterministic tests for Milestone 4 entity labeling."""

import csv
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from chainlens.labels.logic import (
    derive_label_snapshot,
    propagation_confidence,
    seed_entity_id,
)
from chainlens.labels.models import ClusterMember, SeedLabel
from chainlens.labels.seeds import REQUIRED_COLUMNS, load_seed_file, seed_dataset_version
from chainlens.labels.taxonomy import TAXONOMY_VERSION


CHAIN_ID = 8453
CLUSTER_RUN_ID = UUID("11111111-1111-1111-1111-111111111111")


def address(value: int) -> str:
    return f"0x{value:040x}"


def seed(
    value: int,
    entity_name: str = "Uniswap",
    category: str = "dex",
    role: str | None = "router",
    confidence: float = 1.0,
) -> SeedLabel:
    return SeedLabel(
        CHAIN_ID,
        address(value),
        entity_name,
        category,
        role,
        "official_docs",
        f"reference-{value}",
        confidence,
        f"curated-{value}",
    )


def cluster(value: int, cluster_id: str = "candidate-1", confidence: float = 0.8) -> ClusterMember:
    return ClusterMember(CHAIN_ID, cluster_id, address(value), confidence, CLUSTER_RUN_ID)


def write_seed_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def csv_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "chain_id": CHAIN_ID,
        "address": address(1),
        "entity_name": "Uniswap",
        "entity_category": "dex",
        "contract_role": "router",
        "source_type": "official_docs",
        "source_reference": "official deployment list",
        "confidence": 1.0,
        "notes": "SwapRouter02",
    }
    row.update(changes)
    return row


def test_valid_seed_file_loads_and_normalizes(tmp_path: Path) -> None:
    path = tmp_path / "seeds.csv"
    write_seed_csv(path, [csv_row(address=address(10).upper().replace("0X", "0x"))])
    loaded = load_seed_file(path, chain_id=CHAIN_ID)
    assert loaded[0].address == address(10)
    assert loaded[0].contract_role == "router"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"address": "0xnope"}, "invalid address"),
        ({"entity_category": "mev"}, "unsupported entity_category"),
        ({"contract_role": "sniper"}, "unsupported contract_role"),
        ({"confidence": 1.01}, "confidence must be"),
    ],
)
def test_invalid_seed_values_fail(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    path = tmp_path / "seeds.csv"
    write_seed_csv(path, [csv_row(**change)])
    with pytest.raises(ValueError, match=message):
        load_seed_file(path)


def test_conflicting_duplicate_seed_address_fails(tmp_path: Path) -> None:
    path = tmp_path / "seeds.csv"
    write_seed_csv(path, [csv_row(), csv_row(entity_name="Aerodrome")])
    with pytest.raises(ValueError, match="conflicting duplicate"):
        load_seed_file(path)


def test_identical_duplicate_seed_address_is_reconciled(tmp_path: Path) -> None:
    path = tmp_path / "seeds.csv"
    write_seed_csv(path, [csv_row(), csv_row()])
    assert len(load_seed_file(path)) == 1


def test_entity_id_is_deterministic_and_normalized() -> None:
    assert seed_entity_id(CHAIN_ID, " Uniswap ") == seed_entity_id(CHAIN_ID, "uniswap")
    assert seed_entity_id(CHAIN_ID, "Uniswap") != seed_entity_id(CHAIN_ID + 1, "Uniswap")


def test_direct_router_has_category_role_and_provenance() -> None:
    snapshot = derive_label_snapshot(
        CHAIN_ID, [seed(1)], [], taxonomy_version=TAXONOMY_VERSION, propagation_factor=0.95
    )
    direct = [item for item in snapshot.assignments if item.subject_id == address(1)]
    assert {(item.label_dimension, item.label_value) for item in direct} == {
        ("entity_category", "dex"),
        ("contract_role", "router"),
    }
    assert all(item.assignment_method == "direct_seed" for item in direct)
    assert all(item.source_reference == "reference-1" for item in direct)
    assert len([item for item in snapshot.evidence if item.subject_id == address(1)]) == 2


def test_addresses_share_entity_but_retain_distinct_roles() -> None:
    snapshot = derive_label_snapshot(
        CHAIN_ID,
        [seed(1, role="router"), seed(2, role="factory")],
        [],
        taxonomy_version=TAXONOMY_VERSION,
        propagation_factor=0.95,
    )
    direct_members = [item for item in snapshot.members if item.membership_method == "direct_seed"]
    assert len({item.entity_id for item in direct_members}) == 1
    roles = {
        item.subject_id: item.label_value
        for item in snapshot.assignments
        if item.label_dimension == "contract_role"
    }
    assert roles == {address(1): "router", address(2): "factory"}


def test_interaction_alone_never_propagates_a_label() -> None:
    snapshot = derive_label_snapshot(
        CHAIN_ID, [seed(1)], [], taxonomy_version=TAXONOMY_VERSION, propagation_factor=0.95
    )
    assert address(2) not in {item.subject_id for item in snapshot.assignments}


def test_cluster_propagates_only_entity_category_with_exact_confidence() -> None:
    snapshot = derive_label_snapshot(
        CHAIN_ID,
        [seed(1, confidence=0.9)],
        [cluster(1, confidence=0.8), cluster(2, confidence=0.7), cluster(3, confidence=0.6)],
        taxonomy_version=TAXONOMY_VERSION,
        propagation_factor=0.95,
    )
    propagated = [
        item for item in snapshot.assignments if item.assignment_method == "cluster_propagation"
    ]
    assert {(item.subject_type, item.subject_id) for item in propagated} == {
        ("entity", next(item.entity_id for item in snapshot.entities if item.created_from == "cluster_membership")),
        ("address", address(2)),
        ("address", address(3)),
    }
    assert all(item.label_dimension == "entity_category" for item in propagated)
    assert next(item.confidence for item in propagated if item.subject_id == address(2)) == pytest.approx(0.9 * 0.7 * 0.95)
    assert not any(
        item.subject_id in {address(2), address(3)} and item.label_dimension == "contract_role"
        for item in snapshot.assignments
    )
    assert propagation_confidence(1.0, 0.9569, 0.95) == pytest.approx(0.909055)


def test_conflicting_cluster_records_conflict_and_blocks_propagation() -> None:
    snapshot = derive_label_snapshot(
        CHAIN_ID,
        [seed(1, "Uniswap"), seed(2, "Aerodrome")],
        [cluster(1), cluster(2), cluster(3)],
        taxonomy_version=TAXONOMY_VERSION,
        propagation_factor=0.95,
    )
    assert len(snapshot.conflicts) == 1
    assert snapshot.conflicts[0].conflict_type == "conflicting_direct_seed_identities"
    assert not any(
        item.assignment_method == "cluster_propagation" for item in snapshot.assignments
    )
    assert address(3) not in {item.subject_id for item in snapshot.assignments}


def test_derivation_and_dataset_versions_are_deterministic() -> None:
    seeds = [seed(1), seed(2, role="factory")]
    clusters = [cluster(1), cluster(3)]
    first = derive_label_snapshot(
        CHAIN_ID, seeds, clusters, taxonomy_version=TAXONOMY_VERSION, propagation_factor=0.95
    )
    second = derive_label_snapshot(
        CHAIN_ID,
        list(reversed(seeds)),
        list(reversed(clusters)),
        taxonomy_version=TAXONOMY_VERSION,
        propagation_factor=0.95,
    )
    assert first == second
    assert seed_dataset_version(seeds) == seed_dataset_version(list(reversed(seeds)))
    assert seed_dataset_version(seeds) != seed_dataset_version(
        [replace(seeds[0], notes="changed"), seeds[1]]
    )
