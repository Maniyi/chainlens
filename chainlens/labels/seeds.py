"""Load, normalize, validate, and fingerprint curated label seeds."""

import csv
import json
from hashlib import sha256
from pathlib import Path

from chainlens.clustering.logic import is_valid_wallet_address
from chainlens.labels.models import SeedLabel
from chainlens.labels.taxonomy import TAXONOMY


REQUIRED_COLUMNS = (
    "chain_id",
    "address",
    "entity_name",
    "entity_category",
    "contract_role",
    "source_type",
    "source_reference",
    "confidence",
    "notes",
)


def _clean(value: str | None) -> str:
    return (value or "").strip()


def load_seed_file(path: str | Path, *, chain_id: int | None = None) -> list[SeedLabel]:
    """Return normalized seeds, rejecting ambiguous duplicate addresses."""

    source = Path(path)
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != list(REQUIRED_COLUMNS):
            raise ValueError(
                "seed CSV columns must be exactly: " + ",".join(REQUIRED_COLUMNS)
            )
        rows = list(reader)

    seeds: dict[tuple[int, str], SeedLabel] = {}
    for line_number, row in enumerate(rows, start=2):
        try:
            row_chain_id = int(_clean(row["chain_id"]))
            confidence = float(_clean(row["confidence"]))
        except ValueError as exc:
            raise ValueError(f"invalid numeric value on seed line {line_number}") from exc
        address = _clean(row["address"]).lower()
        entity_name = " ".join(_clean(row["entity_name"]).split())
        category = _clean(row["entity_category"]).lower()
        role = _clean(row["contract_role"]).lower() or None
        source_type = _clean(row["source_type"]).lower()
        source_reference = _clean(row["source_reference"])
        if row_chain_id <= 0 or (chain_id is not None and row_chain_id != chain_id):
            raise ValueError(f"invalid or unexpected chain_id on seed line {line_number}")
        if not is_valid_wallet_address(address):
            raise ValueError(f"invalid address on seed line {line_number}: {address}")
        if not entity_name:
            raise ValueError(f"entity_name is required on seed line {line_number}")
        if category not in TAXONOMY["entity_category"]:
            raise ValueError(f"unsupported entity_category on seed line {line_number}: {category}")
        if role is not None and role not in TAXONOMY["contract_role"]:
            raise ValueError(f"unsupported contract_role on seed line {line_number}: {role}")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"confidence must be in [0,1] on seed line {line_number}")
        if not source_type or not source_reference:
            raise ValueError(f"source provenance is required on seed line {line_number}")
        seed = SeedLabel(
            row_chain_id,
            address,
            entity_name,
            category,
            role,
            source_type,
            source_reference,
            confidence,
            _clean(row["notes"]),
        )
        key = (row_chain_id, address)
        previous = seeds.get(key)
        if previous is not None and previous != seed:
            raise ValueError(f"conflicting duplicate seed address: {address}")
        seeds[key] = seed
    return sorted(seeds.values(), key=lambda item: (item.chain_id, item.address))


def seed_dataset_version(seeds: list[SeedLabel]) -> str:
    """Hash a canonical semantic representation, independent of CSV row order."""

    normalized = [
        {
            "address": item.address,
            "chain_id": item.chain_id,
            "confidence": format(item.confidence, ".17g"),
            "contract_role": item.contract_role or "",
            "entity_category": item.entity_category,
            "entity_name": item.entity_name,
            "notes": item.notes,
            "source_reference": item.source_reference,
            "source_type": item.source_type,
        }
        for item in sorted(seeds, key=lambda value: (value.chain_id, value.address))
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return "sha256:" + sha256(payload.encode("utf-8")).hexdigest()
