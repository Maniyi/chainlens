"""ClickHouse persistence matching the Milestone 1 append/version schema."""

from datetime import UTC, datetime
from uuid import UUID

from clickhouse_connect.driver.client import Client

from chainlens.evm.models import EvmBlock, TokenTransfer, parse_quantity
from chainlens.ingestion.models import CanonicalBlock, Checkpoint, ReorgPlan


class ClickHouseStore:
    """Focused persistence adapter; current-state reads never rely on merges."""

    def __init__(self, client: Client) -> None:
        self.client = client

    def write_raw_facts(
        self,
        chain_id: int,
        block: EvmBlock,
        receipts: list[dict[str, object]],
        transfers: list[TokenTransfer],
        ingested_at: datetime,
    ) -> None:
        """Write replayable raw facts in block, transaction, transfer order."""

        self.client.insert(
            "chainlens.raw_blocks",
            [
                (
                    chain_id,
                    block.number,
                    block.block_hash,
                    block.parent_hash,
                    block.timestamp,
                    len(block.transactions),
                    ingested_at,
                )
            ],
            column_names=[
                "chain_id",
                "block_number",
                "block_hash",
                "parent_hash",
                "block_timestamp",
                "transaction_count",
                "ingested_at",
            ],
        )
        if block.transactions:
            statuses = [
                parse_quantity(receipt["status"])
                if receipt.get("status") is not None
                else None
                for receipt in receipts
            ]
            self.client.insert(
                "chainlens.raw_transactions",
                [
                    (
                        chain_id,
                        block.number,
                        block.block_hash,
                        block.timestamp,
                        tx.tx_hash,
                        tx.tx_index,
                        tx.from_address,
                        tx.to_address,
                        tx.value_wei,
                        tx.gas,
                        tx.gas_price,
                        tx.input,
                        status,
                        ingested_at,
                    )
                    for tx, status in zip(block.transactions, statuses, strict=True)
                ],
                column_names=[
                    "chain_id",
                    "block_number",
                    "block_hash",
                    "block_timestamp",
                    "tx_hash",
                    "tx_index",
                    "from_address",
                    "to_address",
                    "value_wei",
                    "gas",
                    "gas_price",
                    "input",
                    "status",
                    "ingested_at",
                ],
            )
        if transfers:
            self.client.insert(
                "chainlens.token_transfers",
                [
                    (
                        transfer.chain_id,
                        transfer.block_number,
                        transfer.block_hash,
                        transfer.block_timestamp,
                        transfer.tx_hash,
                        transfer.log_index,
                        transfer.token_address,
                        transfer.from_address,
                        transfer.to_address,
                        transfer.amount_raw,
                        transfer.ingested_at,
                    )
                    for transfer in transfers
                ],
                column_names=[
                    "chain_id",
                    "block_number",
                    "block_hash",
                    "block_timestamp",
                    "tx_hash",
                    "log_index",
                    "token_address",
                    "from_address",
                    "to_address",
                    "amount_raw",
                    "ingested_at",
                ],
            )

    def get_canonical_block(
        self, chain_id: int, block_number: int
    ) -> CanonicalBlock | None:
        rows = self.client.query(
            "SELECT chain_id, block_number, block_hash, parent_hash, "
            "security_level, version FROM chainlens.current_canonical_blocks "
            "WHERE chain_id = {chain_id:UInt64} AND block_number = {block_number:UInt64}",
            parameters={"chain_id": chain_id, "block_number": block_number},
        ).result_rows
        return CanonicalBlock(*rows[0]) if rows else None

    def list_canonical_blocks(
        self, chain_id: int, start_block: int, end_block: int
    ) -> list[CanonicalBlock]:
        rows = self.client.query(
            "SELECT chain_id, block_number, block_hash, parent_hash, "
            "security_level, version FROM chainlens.current_canonical_blocks "
            "WHERE chain_id = {chain_id:UInt64} "
            "AND block_number BETWEEN {start:UInt64} AND {end:UInt64} "
            "ORDER BY block_number",
            parameters={"chain_id": chain_id, "start": start_block, "end": end_block},
        ).result_rows
        return [CanonicalBlock(*row) for row in rows]

    def get_canonical_head(self, chain_id: int) -> CanonicalBlock | None:
        rows = self.client.query(
            "SELECT chain_id, block_number, block_hash, parent_hash, "
            "security_level, version FROM chainlens.current_canonical_blocks "
            "WHERE chain_id = {chain_id:UInt64} ORDER BY block_number DESC LIMIT 1",
            parameters={"chain_id": chain_id},
        ).result_rows
        return CanonicalBlock(*rows[0]) if rows else None

    def set_canonical(
        self,
        chain_id: int,
        block: EvmBlock,
        security_level: str,
        reason: str,
        observed_at: datetime,
    ) -> bool:
        """Append changed current state followed by its canonical history record."""

        current = self.get_canonical_block(chain_id, block.number)
        if (
            current is not None
            and current.block_hash == block.block_hash
            and current.parent_hash == block.parent_hash
            and current.security_level == security_level
        ):
            return False
        version = (current.version if current else 0) + 1
        self.client.insert(
            "chainlens.canonical_blocks",
            [
                (
                    chain_id,
                    block.number,
                    block.block_hash,
                    block.parent_hash,
                    security_level,
                    observed_at,
                    version,
                )
            ],
            column_names=[
                "chain_id",
                "block_number",
                "block_hash",
                "parent_hash",
                "security_level",
                "observed_at",
                "version",
            ],
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

    def append_history(
        self,
        *,
        chain_id: int,
        block_number: int,
        block_hash: str,
        parent_hash: str,
        security_level: str,
        canonical: bool,
        reason: str,
        detected_at: datetime,
    ) -> None:
        sequence = self.client.query(
            "SELECT coalesce(max(sequence), 0) + 1 "
            "FROM chainlens.canonical_blocks_history "
            "WHERE chain_id = {chain_id:UInt64} AND block_number = {block_number:UInt64}",
            parameters={"chain_id": chain_id, "block_number": block_number},
        ).first_row[0]
        self.client.insert(
            "chainlens.canonical_blocks_history",
            [
                (
                    chain_id,
                    block_number,
                    block_hash,
                    parent_hash,
                    security_level,
                    int(canonical),
                    reason,
                    detected_at,
                    sequence,
                )
            ],
            column_names=[
                "chain_id",
                "block_number",
                "block_hash",
                "parent_hash",
                "security_level",
                "canonical",
                "reason",
                "detected_at",
                "sequence",
            ],
        )

    def get_checkpoint(self, job_name: str, chain_id: int) -> Checkpoint | None:
        rows = self.client.query(
            "SELECT tupleElement(state, 1), tupleElement(state, 2), max_version FROM ("
            "SELECT argMax(tuple(last_processed_block, last_processed_block_hash), version) AS state, "
            "max(version) AS max_version FROM chainlens.pipeline_checkpoints "
            "WHERE job_name = {job_name:String} AND chain_id = {chain_id:UInt64}) "
            "WHERE max_version > 0",
            parameters={"job_name": job_name, "chain_id": chain_id},
        ).result_rows
        if not rows:
            return None
        return Checkpoint(job_name, chain_id, rows[0][0], rows[0][1], rows[0][2])

    def write_checkpoint(
        self, job_name: str, chain_id: int, block_number: int, block_hash: str
    ) -> Checkpoint:
        current = self.get_checkpoint(job_name, chain_id)
        if (
            current is not None
            and current.block_number == block_number
            and current.block_hash == block_hash
        ):
            return current
        version = (current.version if current else 0) + 1
        updated_at = datetime.now(UTC)
        self.client.insert(
            "chainlens.pipeline_checkpoints",
            [(job_name, chain_id, block_number, block_hash, updated_at, version)],
            column_names=[
                "job_name",
                "chain_id",
                "last_processed_block",
                "last_processed_block_hash",
                "updated_at",
                "version",
            ],
        )
        return Checkpoint(job_name, chain_id, block_number, block_hash, version)

    def write_reorg_event(
        self, chain_id: int, plan: ReorgPlan, detected_at: datetime
    ) -> None:
        from uuid import uuid4

        self.client.insert(
            "chainlens.reorg_events",
            [
                (
                    uuid4(),
                    chain_id,
                    detected_at,
                    plan.new_head.number,
                    plan.old_head.block_number,
                    plan.old_head.block_hash,
                    plan.new_head.number,
                    plan.new_head.block_hash,
                    plan.common_ancestor_block,
                    plan.common_ancestor_hash,
                    len(plan.orphaned_blocks),
                    len(plan.replacement_blocks),
                )
            ],
            column_names=[
                "reorg_id",
                "chain_id",
                "detected_at",
                "detected_at_block",
                "old_head_block",
                "old_head_hash",
                "new_head_block",
                "new_head_hash",
                "common_ancestor_block",
                "common_ancestor_hash",
                "orphaned_block_count",
                "replacement_block_count",
            ],
        )

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
    ) -> None:
        now = datetime.now(UTC)
        self.client.insert(
            "chainlens.pipeline_runs",
            [
                (
                    run_id,
                    job_name,
                    chain_id,
                    mode,
                    start_block,
                    end_block,
                    status,
                    started_at,
                    now if status in {"completed", "failed"} else None,
                    error,
                    now,
                    version,
                )
            ],
            column_names=[
                "run_id",
                "job_name",
                "chain_id",
                "mode",
                "start_block",
                "end_block",
                "status",
                "started_at",
                "completed_at",
                "error",
                "updated_at",
                "version",
            ],
        )
