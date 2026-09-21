"""Normalized EVM models used by ingestion."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


class EvmDataError(ValueError):
    """Raised when an RPC response is internally inconsistent or malformed."""


def parse_quantity(value: str | int) -> int:
    """Parse a JSON-RPC hexadecimal quantity (or an already parsed integer)."""

    if isinstance(value, int):
        if value < 0:
            raise EvmDataError("Ethereum quantities cannot be negative")
        return value
    if not isinstance(value, str) or not value.startswith("0x"):
        raise EvmDataError(f"invalid Ethereum quantity: {value!r}")
    try:
        parsed = int(value, 16)
    except ValueError as exc:
        raise EvmDataError(f"invalid Ethereum quantity: {value!r}") from exc
    if parsed < 0:
        raise EvmDataError("Ethereum quantities cannot be negative")
    return parsed


def normalize_hex(value: str, *, field_name: str) -> str:
    """Validate and lowercase a 0x-prefixed hexadecimal value."""

    if not isinstance(value, str) or not value.startswith("0x"):
        raise EvmDataError(f"{field_name} must be 0x-prefixed hex")
    try:
        bytes.fromhex(value[2:])
    except ValueError as exc:
        raise EvmDataError(f"{field_name} is not valid hex") from exc
    return value.lower()


def normalize_address(value: str, *, field_name: str = "address") -> str:
    """Validate and lowercase a 20-byte EVM address."""

    normalized = normalize_hex(value, field_name=field_name)
    if len(normalized) != 42:
        raise EvmDataError(f"{field_name} must contain 20 bytes")
    return normalized


@dataclass(frozen=True)
class EvmTransaction:
    tx_hash: str
    tx_index: int
    from_address: str
    to_address: str | None
    value_wei: int
    gas: int
    gas_price: int | None
    input: str


@dataclass(frozen=True)
class EvmBlock:
    number: int
    block_hash: str
    parent_hash: str
    timestamp: datetime
    transactions: tuple[EvmTransaction, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class TokenTransfer:
    chain_id: int
    block_number: int
    block_hash: str
    block_timestamp: datetime
    tx_hash: str
    log_index: int
    token_address: str
    from_address: str
    to_address: str
    amount_raw: int
    ingested_at: datetime


def normalize_transaction(payload: dict[str, Any]) -> EvmTransaction:
    """Normalize one full transaction object returned with a block."""

    to_value = payload.get("to")
    gas_price = payload.get("gasPrice")
    return EvmTransaction(
        tx_hash=normalize_hex(payload["hash"], field_name="transaction hash"),
        tx_index=parse_quantity(payload["transactionIndex"]),
        from_address=normalize_address(payload["from"], field_name="from address"),
        to_address=(
            normalize_address(to_value, field_name="to address")
            if to_value is not None
            else None
        ),
        value_wei=parse_quantity(payload["value"]),
        gas=parse_quantity(payload["gas"]),
        gas_price=parse_quantity(gas_price) if gas_price is not None else None,
        input=normalize_hex(payload.get("input", "0x"), field_name="transaction input"),
    )


def normalize_block(payload: dict[str, Any]) -> EvmBlock:
    """Normalize a full JSON-RPC block response."""

    transactions = payload.get("transactions", [])
    if any(not isinstance(tx, dict) for tx in transactions):
        raise EvmDataError("block response did not include full transaction objects")
    return EvmBlock(
        number=parse_quantity(payload["number"]),
        block_hash=normalize_hex(payload["hash"], field_name="block hash"),
        parent_hash=normalize_hex(payload["parentHash"], field_name="parent hash"),
        timestamp=datetime.fromtimestamp(parse_quantity(payload["timestamp"]), tz=UTC),
        transactions=tuple(normalize_transaction(tx) for tx in transactions),
    )
