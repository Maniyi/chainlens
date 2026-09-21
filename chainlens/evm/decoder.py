"""Strict decoding for standard ERC-20 Transfer logs."""

from datetime import datetime
from typing import Any

from chainlens.evm.models import TokenTransfer, normalize_address, normalize_hex, parse_quantity


TRANSFER_TOPIC0 = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)


def decode_transfer_log(
    log: dict[str, Any],
    *,
    chain_id: int,
    block_number: int,
    block_hash: str,
    block_timestamp: datetime,
    tx_hash: str,
    ingested_at: datetime,
) -> TokenTransfer | None:
    """Decode a structurally standard Transfer log, otherwise return ``None``."""

    topics = log.get("topics")
    data = log.get("data")
    if not isinstance(topics, list) or len(topics) != 3:
        return None
    if not isinstance(topics[0], str) or topics[0].lower() != TRANSFER_TOPIC0:
        return None
    if not all(isinstance(topic, str) and len(topic) == 66 for topic in topics[1:]):
        return None
    if not isinstance(data, str) or len(data) != 66:
        return None
    try:
        from_address = normalize_address("0x" + topics[1][-40:], field_name="from topic")
        to_address = normalize_address("0x" + topics[2][-40:], field_name="to topic")
        amount_raw = parse_quantity(data)
        token_address = normalize_address(log["address"], field_name="token address")
        log_index = parse_quantity(log["logIndex"])
        normalized_tx_hash = normalize_hex(tx_hash, field_name="transaction hash")
    except (KeyError, TypeError, ValueError):
        return None
    return TokenTransfer(
        chain_id=chain_id,
        block_number=block_number,
        block_hash=block_hash,
        block_timestamp=block_timestamp,
        tx_hash=normalized_tx_hash,
        log_index=log_index,
        token_address=token_address,
        from_address=from_address,
        to_address=to_address,
        amount_raw=amount_raw,
        ingested_at=ingested_at,
    )
