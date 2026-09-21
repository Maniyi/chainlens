"""Canonical ancestry and Base safe/finalized classification."""

from datetime import UTC, datetime
from typing import Any, Protocol

from chainlens.evm.models import EvmBlock, normalize_block
from chainlens.ingestion.models import CanonicalBlock, FinalityHeads


class HeadRpc(Protocol):
    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]: ...


class CanonicalStore(Protocol):
    def list_canonical_blocks(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalBlock]: ...

    def set_canonical(
        self,
        chain_id: int,
        block: EvmBlock,
        security_level: str,
        reason: str,
        observed_at: datetime,
    ) -> bool: ...


def parent_is_continuous(previous_hash: str, next_parent_hash: str) -> bool:
    """Return whether two adjacent blocks form a direct ancestry edge."""

    return previous_hash.lower() == next_parent_hash.lower()


def fetch_finality_heads(rpc: HeadRpc) -> FinalityHeads:
    """Read protocol-provided latest, safe, and finalized heads."""

    latest = normalize_block(rpc.get_block_by_number("latest", full_transactions=True))
    safe = normalize_block(rpc.get_block_by_number("safe", full_transactions=True))
    finalized = normalize_block(
        rpc.get_block_by_number("finalized", full_transactions=True)
    )
    if not (finalized.number <= safe.number <= latest.number):
        raise ValueError(
            "RPC returned inconsistent finality heads: "
            f"finalized={finalized.number}, safe={safe.number}, latest={latest.number}"
        )
    return FinalityHeads(latest.number, safe.number, finalized.number)


def classify_security(block_number: int, heads: FinalityHeads) -> str:
    if block_number <= heads.finalized:
        return "finalized"
    if block_number <= heads.safe:
        return "safe"
    return "unsafe"


def refresh_security_levels(
    store: CanonicalStore,
    chain_id: int,
    start_block: int,
    end_block: int,
    heads: FinalityHeads,
) -> int:
    """Append security upgrades for current canonical blocks in a bounded range."""

    rank = {"unsafe": 1, "safe": 2, "finalized": 3}
    changed = 0
    now = datetime.now(UTC)
    for current in store.list_canonical_blocks(chain_id, start_block, end_block):
        desired = classify_security(current.block_number, heads)
        if rank[desired] <= rank[current.security_level]:
            continue
        block = EvmBlock(
            current.block_number,
            current.block_hash,
            current.parent_hash,
            now,
        )
        changed += int(
            store.set_canonical(
                chain_id, block, desired, "security_upgrade", observed_at=now
            )
        )
    return changed

