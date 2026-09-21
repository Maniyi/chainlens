"""Checkpoint resume validation helpers."""

from typing import Any, Protocol

from chainlens.evm.models import normalize_block
from chainlens.ingestion.models import Checkpoint


class CheckpointRpc(Protocol):
    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]: ...


def checkpoint_matches_rpc(checkpoint: Checkpoint, rpc: CheckpointRpc) -> bool:
    """Verify a checkpoint against the hash currently served at that exact height."""

    block = normalize_block(
        rpc.get_block_by_number(checkpoint.block_number, full_transactions=True)
    )
    return block.block_hash == checkpoint.block_hash.lower()

