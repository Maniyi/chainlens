"""Unit tests for EVM normalization, RPC encoding, and Transfer decoding."""

from datetime import UTC, datetime

import httpx

from chainlens.evm.decoder import TRANSFER_TOPIC0, decode_transfer_log
from chainlens.evm.models import normalize_block, parse_quantity
from chainlens.evm.rpc import EthereumRpcClient, encode_block_identifier


def test_rpc_quantities_and_block_normalization() -> None:
    payload = {
        "number": "0x7b",
        "hash": "0x" + "AB" * 32,
        "parentHash": "0x" + "CD" * 32,
        "timestamp": "0x64",
        "transactions": [
            {
                "hash": "0x" + "EF" * 32,
                "transactionIndex": "0x2",
                "from": "0x" + "11" * 20,
                "to": None,
                "value": "0x10",
                "gas": "0x5208",
                "gasPrice": "0x3b9aca00",
                "input": "0xAABB",
            }
        ],
    }

    block = normalize_block(payload)

    assert parse_quantity("0x10") == 16
    assert encode_block_identifier(123) == "0x7b"
    assert encode_block_identifier("safe") == "safe"
    assert block.number == 123
    assert block.block_hash == ("0x" + "ab" * 32)
    assert block.transactions[0].to_address is None
    assert block.transactions[0].input == "0xaabb"


def test_decodes_standard_erc20_transfer_log() -> None:
    from_address = "12" * 20
    to_address = "34" * 20
    log = {
        "address": "0x" + "56" * 20,
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + "00" * 12 + from_address,
            "0x" + "00" * 12 + to_address,
        ],
        "data": "0x" + (123456).to_bytes(32, "big").hex(),
        "logIndex": "0x7",
    }
    now = datetime.now(UTC)

    transfer = decode_transfer_log(
        log,
        chain_id=8453,
        block_number=100,
        block_hash="0x" + "ab" * 32,
        block_timestamp=now,
        tx_hash="0x" + "cd" * 32,
        ingested_at=now,
    )

    assert transfer is not None
    assert transfer.from_address == "0x" + from_address
    assert transfer.to_address == "0x" + to_address
    assert transfer.token_address == "0x" + "56" * 20
    assert transfer.amount_raw == 123456
    assert transfer.log_index == 7


def test_skips_malformed_transfer_log() -> None:
    malformed = {
        "address": "0x" + "56" * 20,
        "topics": [TRANSFER_TOPIC0],
        "data": "0x01",
        "logIndex": "0x0",
    }
    now = datetime.now(UTC)
    assert (
        decode_transfer_log(
            malformed,
            chain_id=8453,
            block_number=1,
            block_hash="0x" + "ab" * 32,
            block_timestamp=now,
            tx_hash="0x" + "cd" * 32,
            ingested_at=now,
        )
        is None
    )


def test_rpc_client_retries_bounded_server_failure() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, request=request)
        request_id = request.read().decode()
        assert '"method":"eth_chainId"' in request_id
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": "0x2105"},
            request=request,
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    rpc = EthereumRpcClient(
        "https://rpc.invalid",
        client=http_client,
        max_attempts=2,
        backoff_seconds=0,
    )

    assert rpc.chain_id() == 8453
    assert attempts == 2
    http_client.close()
