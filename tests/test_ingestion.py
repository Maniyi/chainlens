"""Deterministic ingestion and reorg tests using an in-memory store and RPC."""

from datetime import UTC, datetime
from typing import Any

from chainlens.evm.models import EvmBlock
from chainlens.ingestion.canonicality import parent_is_continuous
from chainlens.ingestion.models import CanonicalBlock, Checkpoint
from chainlens.ingestion.reorg import find_common_ancestor
from chainlens.ingestion.runner import BackfillRunner


def block_payload(number: int, block_hash: str, parent_hash: str) -> dict[str, Any]:
    return {
        "number": hex(number),
        "hash": block_hash,
        "parentHash": parent_hash,
        "timestamp": hex(1_700_000_000 + number),
        "transactions": [],
    }


class FakeRpc:
    def __init__(self, blocks: dict[int, dict[str, Any]]) -> None:
        self.blocks = blocks

    def chain_id(self) -> int:
        return 8453

    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]:
        if block == "latest":
            return self.blocks[max(self.blocks)]
        if block == "safe":
            return self.blocks[max(self.blocks) - 1]
        if block == "finalized":
            return self.blocks[min(self.blocks)]
        assert isinstance(block, int)
        return self.blocks[block]

    def get_transaction_receipts(
        self, tx_hashes: list[str], *, batch_size: int = 20
    ) -> list[dict[str, Any]]:
        assert tx_hashes == []
        return []


class MemoryStore:
    def __init__(self) -> None:
        self.canonical: dict[int, CanonicalBlock] = {}
        self.raw_blocks: set[tuple[int, str]] = set()
        self.history: list[dict[str, Any]] = []
        self.events: list[Any] = []
        self.checkpoint: Checkpoint | None = None
        self.run_states: list[str] = []

    def write_raw_facts(self, chain_id, block, receipts, transfers, ingested_at) -> None:
        self.raw_blocks.add((block.number, block.block_hash))

    def get_canonical_block(self, chain_id: int, block_number: int):
        return self.canonical.get(block_number)

    def get_canonical_head(self, chain_id: int):
        return self.canonical[max(self.canonical)] if self.canonical else None

    def list_canonical_blocks(self, chain_id: int, start_block: int, end_block: int):
        return [
            self.canonical[number]
            for number in sorted(self.canonical)
            if start_block <= number <= end_block
        ]

    def set_canonical(
        self, chain_id, block, security_level, reason, observed_at
    ) -> bool:
        current = self.canonical.get(block.number)
        if (
            current is not None
            and current.block_hash == block.block_hash
            and current.security_level == security_level
        ):
            return False
        version = (current.version if current else 0) + 1
        self.canonical[block.number] = CanonicalBlock(
            chain_id,
            block.number,
            block.block_hash,
            block.parent_hash,
            security_level,
            version,
        )
        self.append_history(
            chain_id=chain_id,
            block_number=block.number,
            block_hash=block.block_hash,
            parent_hash=block.parent_hash,
            security_level=security_level,
            canonical=True,
            reason=reason,
            detected_at=observed_at,
        )
        return True

    def append_history(self, **values) -> None:
        self.history.append(values)

    def get_checkpoint(self, job_name: str, chain_id: int):
        return self.checkpoint

    def write_checkpoint(self, job_name, chain_id, block_number, block_hash):
        if (
            self.checkpoint
            and self.checkpoint.block_number == block_number
            and self.checkpoint.block_hash == block_hash
        ):
            return self.checkpoint
        version = (self.checkpoint.version if self.checkpoint else 0) + 1
        self.checkpoint = Checkpoint(
            job_name, chain_id, block_number, block_hash, version
        )
        return self.checkpoint

    def write_reorg_event(self, chain_id, plan, detected_at) -> None:
        self.events.append(plan)

    def write_run_state(
        self,
        run_id,
        job_name,
        chain_id,
        mode,
        start_block,
        end_block,
        status,
        started_at,
        version,
        *,
        error=None,
    ) -> None:
        self.run_states.append(status)


def make_branches() -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    h = lambda label: "0x" + label * 64
    old = {
        100: block_payload(100, h("a"), h("0")),
        101: block_payload(101, h("b"), h("a")),
        102: block_payload(102, h("c"), h("b")),
        103: block_payload(103, h("d"), h("c")),
    }
    new = {
        100: old[100],
        101: block_payload(101, h("1"), h("a")),
        102: block_payload(102, h("2"), h("1")),
        103: block_payload(103, h("3"), h("2")),
    }
    return old, new


def test_parent_continuity() -> None:
    assert parent_is_continuous("0xAA", "0xaa")
    assert not parent_is_continuous("0xaa", "0xbb")


def test_common_ancestor_uses_rpc_ancestry() -> None:
    old, new = make_branches()
    store = MemoryStore()
    rpc = FakeRpc(new)
    for number, payload in old.items():
        store.set_canonical(
            8453,
            EvmBlock(
                number,
                payload["hash"],
                payload["parentHash"],
                datetime.now(UTC),
            ),
            "unsafe",
            "observed",
            datetime.now(UTC),
        )

    new_head = EvmBlock(103, new[103]["hash"], new[103]["parentHash"], datetime.now(UTC))
    plan = find_common_ancestor(rpc, store, 8453, new_head, max_depth=10)

    assert plan.common_ancestor_block == 100
    assert [block.block_hash for block in plan.orphaned_blocks] == [
        old[101]["hash"],
        old[102]["hash"],
        old[103]["hash"],
    ]
    assert [block.block_hash for block in plan.replacement_blocks] == [
        new[101]["hash"],
        new[102]["hash"],
        new[103]["hash"],
    ]


def test_recovery_preserves_raw_forks_switches_canonical_and_is_idempotent() -> None:
    old, new = make_branches()
    rpc = FakeRpc(old)
    store = MemoryStore()
    runner = BackfillRunner(rpc, store, chain_id=8453, max_reorg_depth=10)

    first = runner.run(100, 103)
    assert first.blocks_processed == 4
    assert store.checkpoint and store.checkpoint.block_hash == old[103]["hash"]

    rpc.blocks = new
    recovered = runner.run(100, 103)

    assert recovered.reorgs_detected == 1
    assert len(store.events) == 1
    assert store.events[0].common_ancestor_block == 100
    assert len(store.events[0].orphaned_blocks) == 3
    assert len(store.events[0].replacement_blocks) == 3
    assert [store.canonical[number].block_hash for number in range(101, 104)] == [
        new[101]["hash"],
        new[102]["hash"],
        new[103]["hash"],
    ]
    assert {(number, old[number]["hash"]) for number in range(101, 104)} <= store.raw_blocks
    assert {(number, new[number]["hash"]) for number in range(101, 104)} <= store.raw_blocks
    assert store.checkpoint and store.checkpoint.block_hash == new[103]["hash"]
    assert sum(item["reason"] == "reorg_orphaned" for item in store.history) == 3

    logical_snapshot = (dict(store.canonical), set(store.raw_blocks), store.checkpoint)
    replay = runner.run(100, 103)
    assert replay.blocks_processed == 0
    assert replay.reorgs_detected == 0
    assert len(store.events) == 1
    assert (store.canonical, store.raw_blocks, store.checkpoint) == logical_snapshot
