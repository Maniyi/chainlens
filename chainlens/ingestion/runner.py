"""Command-line orchestration for bounded, resumable Base backfills."""

import argparse
import sys
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from chainlens.config import Settings, get_settings
from chainlens.db import create_client
from chainlens.evm.models import EvmBlock
from chainlens.evm.rpc import EthereumRpcClient, RpcError
from chainlens.ingestion.blocks import BlockProcessor, PreparedBlock
from chainlens.ingestion.canonicality import (
    classify_security,
    fetch_finality_heads,
    parent_is_continuous,
    refresh_security_levels,
)
from chainlens.ingestion.models import (
    CanonicalBlock,
    Checkpoint,
    FinalityHeads,
    IngestionSummary,
    ReorgPlan,
)
from chainlens.ingestion.reorg import ReorgDepthExceeded, find_common_ancestor
from chainlens.ingestion.store import ClickHouseStore


JOB_NAME = "block_ingestion"


class RunnerStore(Protocol):
    def get_canonical_block(
        self, chain_id: int, block_number: int
    ) -> CanonicalBlock | None: ...

    def get_canonical_head(self, chain_id: int) -> CanonicalBlock | None: ...

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

    def append_history(self, **values: Any) -> None: ...

    def get_checkpoint(self, job_name: str, chain_id: int) -> Checkpoint | None: ...

    def write_checkpoint(
        self, job_name: str, chain_id: int, block_number: int, block_hash: str
    ) -> Checkpoint: ...

    def write_reorg_event(
        self, chain_id: int, plan: ReorgPlan, detected_at: datetime
    ) -> None: ...

    def write_run_state(
        self,
        run_id: UUID,
        job_name: str,
        chain_id: int,
        mode: str,
        start_block: int,
        end_block: int,
        status: str,
        started_at: datetime,
        version: int,
        *,
        error: str | None = None,
    ) -> None: ...


class RunnerRpc(Protocol):
    def chain_id(self) -> int: ...

    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]: ...

    def get_transaction_receipts(
        self, tx_hashes: list[str], *, batch_size: int = 20
    ) -> list[dict[str, Any]]: ...


class ChainIdMismatch(RuntimeError):
    """The configured endpoint is connected to a different chain."""


class BackfillRunner:
    """Coordinates replay-safe writes, canonicality, recovery, and run state."""

    def __init__(
        self,
        rpc: RunnerRpc,
        store: RunnerStore,
        *,
        chain_id: int,
        receipt_batch_size: int = 20,
        max_reorg_depth: int = 128,
    ) -> None:
        self.rpc = rpc
        self.store = store
        self.chain_id = chain_id
        self.max_reorg_depth = max_reorg_depth
        self.processor = BlockProcessor(
            rpc, store, chain_id, receipt_batch_size=receipt_batch_size
        )

    def _validate_chain(self) -> None:
        actual = self.rpc.chain_id()
        if actual != self.chain_id:
            raise ChainIdMismatch(
                f"RPC chain ID {actual} does not match requested chain ID {self.chain_id}"
            )

    def _requires_reorg(self, block: EvmBlock) -> bool:
        current = self.store.get_canonical_block(self.chain_id, block.number)
        if current is not None and current.block_hash != block.block_hash:
            return True
        if block.number == 0:
            return False
        previous = self.store.get_canonical_block(self.chain_id, block.number - 1)
        return previous is not None and not parent_is_continuous(
            previous.block_hash, block.parent_hash
        )

    def _recover_reorg(
        self,
        new_head: EvmBlock,
        heads: FinalityHeads,
        *,
        prepared_head: PreparedBlock | None = None,
    ) -> tuple[Checkpoint, int, int]:
        plan = find_common_ancestor(
            self.rpc,
            self.store,
            self.chain_id,
            new_head,
            max_depth=self.max_reorg_depth,
        )
        transaction_count = 0
        transfer_count = 0
        for replacement in plan.replacement_blocks:
            prepared = (
                prepared_head
                if prepared_head is not None and replacement.number == new_head.number
                else self.processor.prepare(replacement.number, known_block=replacement)
            )
            self.processor.write(prepared)
            transaction_count += len(prepared.block.transactions)
            transfer_count += len(prepared.transfers)

        detected_at = datetime.now(UTC)
        for orphaned in plan.orphaned_blocks:
            self.store.append_history(
                chain_id=self.chain_id,
                block_number=orphaned.block_number,
                block_hash=orphaned.block_hash,
                parent_hash=orphaned.parent_hash,
                security_level=orphaned.security_level,
                canonical=False,
                reason="reorg_orphaned",
                detected_at=detected_at,
            )
        for replacement in plan.replacement_blocks:
            self.store.set_canonical(
                self.chain_id,
                replacement,
                classify_security(replacement.number, heads),
                "reorg_replacement",
                detected_at,
            )
        self.store.write_reorg_event(self.chain_id, plan, detected_at)
        checkpoint = self.store.write_checkpoint(
            JOB_NAME, self.chain_id, new_head.number, new_head.block_hash
        )
        return checkpoint, transaction_count, transfer_count

    def run(self, from_block: int, to_block: int) -> IngestionSummary:
        if from_block < 0 or to_block < from_block:
            raise ValueError("backfill requires 0 <= from-block <= to-block")

        run_uuid = uuid4()
        started_at = datetime.now(UTC)
        summary = IngestionSummary(str(run_uuid), self.chain_id, from_block, to_block)
        self.store.write_run_state(
            run_uuid,
            JOB_NAME,
            self.chain_id,
            "backfill",
            from_block,
            to_block,
            "pending",
            started_at,
            1,
        )
        self.store.write_run_state(
            run_uuid,
            JOB_NAME,
            self.chain_id,
            "backfill",
            from_block,
            to_block,
            "running",
            started_at,
            2,
        )
        try:
            self._validate_chain()
            heads = fetch_finality_heads(self.rpc)
            checkpoint = self.store.get_checkpoint(JOB_NAME, self.chain_id)
            next_block = from_block
            if checkpoint is not None:
                checkpoint_block = self.processor.fetch(checkpoint.block_number)
                if checkpoint_block.block_hash != checkpoint.block_hash:
                    recovered, transactions, transfers = self._recover_reorg(
                        checkpoint_block, heads
                    )
                    summary.reorgs_detected += 1
                    summary.transactions_inserted += transactions
                    summary.transfers_decoded += transfers
                    summary.final_checkpoint = recovered
                next_block = max(from_block, checkpoint.block_number + 1)

            for block_number in range(next_block, to_block + 1):
                prepared = self.processor.prepare(block_number)
                self.processor.write(prepared)
                if self._requires_reorg(prepared.block):
                    checkpoint, transactions, transfers = self._recover_reorg(
                        prepared.block, heads, prepared_head=prepared
                    )
                    summary.reorgs_detected += 1
                    summary.transactions_inserted += transactions
                    summary.transfers_decoded += transfers
                else:
                    self.store.set_canonical(
                        self.chain_id,
                        prepared.block,
                        classify_security(prepared.block.number, heads),
                        "observed",
                        prepared.ingested_at,
                    )
                    checkpoint = self.store.write_checkpoint(
                        JOB_NAME,
                        self.chain_id,
                        prepared.block.number,
                        prepared.block.block_hash,
                    )
                    summary.transactions_inserted += len(prepared.block.transactions)
                    summary.transfers_decoded += len(prepared.transfers)
                summary.blocks_processed += 1
                summary.final_checkpoint = checkpoint

            refresh_security_levels(
                self.store, self.chain_id, from_block, to_block, heads
            )
            if summary.final_checkpoint is None:
                summary.final_checkpoint = self.store.get_checkpoint(
                    JOB_NAME, self.chain_id
                )
            self.store.write_run_state(
                run_uuid,
                JOB_NAME,
                self.chain_id,
                "backfill",
                from_block,
                to_block,
                "completed",
                started_at,
                3,
            )
            return summary
        except Exception as exc:
            self.store.write_run_state(
                run_uuid,
                JOB_NAME,
                self.chain_id,
                "backfill",
                from_block,
                to_block,
                "failed",
                started_at,
                3,
                error=str(exc)[:4000],
            )
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ChainLens EVM ingestion")
    commands = parser.add_subparsers(dest="command", required=True)
    backfill = commands.add_parser("backfill", help="ingest an explicit block range")
    backfill.add_argument("--chain-id", type=int, required=True)
    backfill.add_argument("--from-block", type=int, required=True)
    backfill.add_argument("--to-block", type=int, required=True)
    return parser


def _print_summary(summary: IngestionSummary) -> None:
    checkpoint = summary.final_checkpoint
    checkpoint_text = (
        f"{checkpoint.block_number}:{checkpoint.block_hash}" if checkpoint else "none"
    )
    print(f"run_id={summary.run_id}")
    print(f"chain_id={summary.chain_id}")
    print(f"range={summary.from_block}-{summary.to_block}")
    print(f"blocks_processed={summary.blocks_processed}")
    print(f"transactions_inserted={summary.transactions_inserted}")
    print(f"transfers_decoded={summary.transfers_decoded}")
    print(f"reorgs_detected={summary.reorgs_detected}")
    print(f"final_checkpoint={checkpoint_text}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings: Settings = get_settings()
    if not settings.base_rpc_url:
        print("BASE_RPC_URL must be configured", file=sys.stderr)
        return 2

    db_client = create_client(settings)
    rpc = EthereumRpcClient(
        settings.base_rpc_url, timeout_seconds=float(settings.rpc_timeout_seconds)
    )
    try:
        runner = BackfillRunner(
            rpc,
            ClickHouseStore(db_client),
            chain_id=args.chain_id,
            receipt_batch_size=int(settings.rpc_batch_size),
            max_reorg_depth=int(settings.reorg_max_depth),
        )
        summary = runner.run(args.from_block, args.to_block)
    except (ValueError, RpcError, ReorgDepthExceeded, ChainIdMismatch) as exc:
        print(f"backfill failed: {exc}", file=sys.stderr)
        return 1
    finally:
        rpc.close()
        db_client.close()
    _print_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
