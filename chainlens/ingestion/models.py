"""Explicit ingestion state and result models."""

from dataclasses import dataclass

from chainlens.evm.models import EvmBlock


SecurityLevel = str


@dataclass(frozen=True)
class CanonicalBlock:
    chain_id: int
    block_number: int
    block_hash: str
    parent_hash: str
    security_level: SecurityLevel
    version: int


@dataclass(frozen=True)
class Checkpoint:
    job_name: str
    chain_id: int
    block_number: int
    block_hash: str
    version: int


@dataclass(frozen=True)
class FinalityHeads:
    latest: int
    safe: int
    finalized: int


@dataclass(frozen=True)
class ReorgPlan:
    common_ancestor_block: int
    common_ancestor_hash: str
    old_head: CanonicalBlock
    new_head: EvmBlock
    orphaned_blocks: tuple[CanonicalBlock, ...]
    replacement_blocks: tuple[EvmBlock, ...]


@dataclass
class IngestionSummary:
    run_id: str
    chain_id: int
    from_block: int
    to_block: int
    blocks_processed: int = 0
    transactions_inserted: int = 0
    transfers_decoded: int = 0
    reorgs_detected: int = 0
    final_checkpoint: Checkpoint | None = None

