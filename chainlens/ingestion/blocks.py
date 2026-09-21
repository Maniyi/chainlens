"""Block preparation and raw fact writes."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from chainlens.evm.decoder import decode_transfer_log
from chainlens.evm.models import EvmBlock, EvmDataError, TokenTransfer, normalize_block


class ReceiptRpc(Protocol):
    def get_block_by_number(
        self, block: int | str, *, full_transactions: bool = True
    ) -> dict[str, Any]: ...

    def get_transaction_receipts(
        self, tx_hashes: list[str], *, batch_size: int = 20
    ) -> list[dict[str, Any]]: ...


class RawFactStore(Protocol):
    def write_raw_facts(
        self,
        chain_id: int,
        block: EvmBlock,
        receipts: list[dict[str, object]],
        transfers: list[TokenTransfer],
        ingested_at: datetime,
    ) -> None: ...


@dataclass(frozen=True)
class PreparedBlock:
    block: EvmBlock
    receipts: list[dict[str, Any]]
    transfers: list[TokenTransfer]
    ingested_at: datetime


class BlockProcessor:
    """Fetch, validate, normalize, decode, and persist one observed block."""

    def __init__(
        self, rpc: ReceiptRpc, store: RawFactStore, chain_id: int, *, receipt_batch_size: int
    ) -> None:
        self.rpc = rpc
        self.store = store
        self.chain_id = chain_id
        self.receipt_batch_size = receipt_batch_size

    def fetch(self, block_number: int) -> EvmBlock:
        return normalize_block(
            self.rpc.get_block_by_number(block_number, full_transactions=True)
        )

    def prepare(self, block_number: int, *, known_block: EvmBlock | None = None) -> PreparedBlock:
        block = known_block or self.fetch(block_number)
        receipts = self.rpc.get_transaction_receipts(
            [tx.tx_hash for tx in block.transactions], batch_size=self.receipt_batch_size
        )
        if len(receipts) != len(block.transactions):
            raise EvmDataError(
                f"block {block.number} returned {len(receipts)} receipts for "
                f"{len(block.transactions)} transactions"
            )
        ingested_at = datetime.now(UTC)
        transfers: list[TokenTransfer] = []
        for transaction, receipt in zip(block.transactions, receipts, strict=True):
            receipt_tx_hash = str(receipt.get("transactionHash", "")).lower()
            receipt_block_hash = str(receipt.get("blockHash", "")).lower()
            if receipt_tx_hash != transaction.tx_hash or receipt_block_hash != block.block_hash:
                raise EvmDataError(
                    f"receipt coordinates do not match block {block.number} transaction "
                    f"{transaction.tx_hash}"
                )
            logs = receipt.get("logs", [])
            if not isinstance(logs, list):
                raise EvmDataError(f"receipt logs for {transaction.tx_hash} are not a list")
            for log in logs:
                if not isinstance(log, dict) or log.get("removed") is True:
                    continue
                transfer = decode_transfer_log(
                    log,
                    chain_id=self.chain_id,
                    block_number=block.number,
                    block_hash=block.block_hash,
                    block_timestamp=block.timestamp,
                    tx_hash=transaction.tx_hash,
                    ingested_at=ingested_at,
                )
                if transfer is not None:
                    transfers.append(transfer)
        return PreparedBlock(block, receipts, transfers, ingested_at)

    def write(self, prepared: PreparedBlock) -> None:
        self.store.write_raw_facts(
            self.chain_id,
            prepared.block,
            prepared.receipts,
            prepared.transfers,
            prepared.ingested_at,
        )

