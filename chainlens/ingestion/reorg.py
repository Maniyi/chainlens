"""Bounded common-ancestor discovery and explicit recovery plans."""

from typing import Any, Protocol

from chainlens.evm.models import EvmBlock, normalize_block
from chainlens.ingestion.models import CanonicalBlock, ReorgPlan


class ReorgDepthExceeded(RuntimeError):
    """No common ancestor was found within the configured search bound."""


class ReorgRpc(Protocol):
    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]: ...


class ReorgStore(Protocol):
    def get_canonical_block(
        self, chain_id: int, block_number: int
    ) -> CanonicalBlock | None: ...

    def get_canonical_head(self, chain_id: int) -> CanonicalBlock | None: ...

    def list_canonical_blocks(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalBlock]: ...


def find_common_ancestor(
    rpc: ReorgRpc,
    store: ReorgStore,
    chain_id: int,
    new_head: EvmBlock,
    *,
    max_depth: int,
) -> ReorgPlan:
    """Compare RPC ancestry with stored canonical hashes and build a recovery plan."""

    old_head = store.get_canonical_head(chain_id)
    if old_head is None:
        raise ReorgDepthExceeded("cannot recover a reorg without a stored canonical branch")
    comparison_height = min(old_head.block_number, new_head.number)
    ancestor: EvmBlock | None = None
    for depth in range(max_depth + 1):
        height = comparison_height - depth
        if height < 0:
            break
        rpc_block = normalize_block(
            rpc.get_block_by_number(height, full_transactions=True)
        )
        canonical = store.get_canonical_block(chain_id, height)
        if canonical is not None and canonical.block_hash == rpc_block.block_hash:
            ancestor = rpc_block
            break
    if ancestor is None:
        raise ReorgDepthExceeded(
            f"no common ancestor found within {max_depth} blocks of height "
            f"{comparison_height}"
        )

    orphaned = tuple(
        store.list_canonical_blocks(
            chain_id, ancestor.number + 1, old_head.block_number
        )
    )
    replacements: list[EvmBlock] = []
    for height in range(ancestor.number + 1, new_head.number + 1):
        replacements.append(
            new_head
            if height == new_head.number
            else normalize_block(
                rpc.get_block_by_number(height, full_transactions=True)
            )
        )
    return ReorgPlan(
        common_ancestor_block=ancestor.number,
        common_ancestor_hash=ancestor.block_hash,
        old_head=old_head,
        new_head=new_head,
        orphaned_blocks=orphaned,
        replacement_blocks=tuple(replacements),
    )
