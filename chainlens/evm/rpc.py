"""Small synchronous Ethereum JSON-RPC client with bounded retries."""

import itertools
import time
from collections.abc import Iterable
from typing import Any, Literal

import httpx

from chainlens.evm.models import parse_quantity


BlockTag = Literal["latest", "safe", "finalized"]


class RpcError(RuntimeError):
    """Base error for RPC transport, protocol, and remote failures."""


class RpcResponseError(RpcError):
    """The endpoint returned a JSON-RPC error response."""

    def __init__(self, method: str, code: int | None, message: str) -> None:
        super().__init__(f"{method} failed ({code}): {message}")
        self.code = code


class RpcNotFoundError(RpcError):
    """A requested chain object was not available from the endpoint."""


def encode_block_identifier(block: int | BlockTag) -> str:
    """Encode a non-negative height or pass through a supported block tag."""

    if isinstance(block, int):
        if block < 0:
            raise ValueError("block number cannot be negative")
        return hex(block)
    if block not in {"latest", "safe", "finalized"}:
        raise ValueError(f"unsupported block tag: {block}")
    return block


class EthereumRpcClient:
    """Minimal synchronous JSON-RPC client suitable for bounded ingestion."""

    def __init__(
        self,
        url: str,
        *,
        timeout_seconds: float = 20.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        client: httpx.Client | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._owns_client = client is None
        self._url = url
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._ids = itertools.count(1)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "EthereumRpcClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _post(self, payload: dict[str, Any] | list[dict[str, Any]]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            try:
                response = self._client.post(self._url, json=payload)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "retryable RPC HTTP status",
                        request=response.request,
                        response=response,
                    )
                if response.is_error:
                    raise RpcError(
                        f"RPC endpoint returned non-retryable HTTP status "
                        f"{response.status_code}"
                    )
                return response.json()
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt + 1 == self._max_attempts:
                    break
                time.sleep(self._backoff_seconds * (2**attempt))
            except ValueError as exc:
                raise RpcError("RPC endpoint returned invalid JSON") from exc
        raise RpcError(f"RPC request failed after {self._max_attempts} attempts") from last_error

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        for attempt in range(self._max_attempts):
            request_id = next(self._ids)
            body = self._post(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params or [],
                }
            )
            if not isinstance(body, dict) or body.get("id") != request_id:
                raise RpcError(f"malformed or mismatched JSON-RPC response for {method}")
            if "error" not in body:
                if "result" not in body:
                    raise RpcError(f"JSON-RPC response for {method} has no result")
                return body["result"]
            error = body["error"] if isinstance(body["error"], dict) else {}
            code = error.get("code")
            rpc_error = RpcResponseError(
                method, code, error.get("message", "unknown error")
            )
            retryable = isinstance(code, int) and -32099 <= code <= -32000
            if not retryable or attempt + 1 == self._max_attempts:
                raise rpc_error
            time.sleep(self._backoff_seconds * (2**attempt))
        raise AssertionError("unreachable")

    def chain_id(self) -> int:
        return parse_quantity(self.call("eth_chainId"))

    def get_block_by_number(
        self, block: int | BlockTag, *, full_transactions: bool = True
    ) -> dict[str, Any]:
        result = self.call(
            "eth_getBlockByNumber", [encode_block_identifier(block), full_transactions]
        )
        if result is None:
            raise RpcNotFoundError(f"block {block!r} was not found")
        if not isinstance(result, dict):
            raise RpcError("eth_getBlockByNumber returned a non-object result")
        return result

    def get_transaction_receipt(self, tx_hash: str) -> dict[str, Any]:
        result = self.call("eth_getTransactionReceipt", [tx_hash])
        if result is None:
            raise RpcNotFoundError(f"receipt for {tx_hash} was not found")
        if not isinstance(result, dict):
            raise RpcError("eth_getTransactionReceipt returned a non-object result")
        return result

    def get_transaction_receipts(
        self, tx_hashes: Iterable[str], *, batch_size: int = 20
    ) -> list[dict[str, Any]]:
        """Fetch receipts in bounded JSON-RPC batches, preserving input order."""

        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        hashes = list(tx_hashes)
        receipts: list[dict[str, Any]] = []
        for offset in range(0, len(hashes), batch_size):
            chunk = hashes[offset : offset + batch_size]
            requests = [
                {
                    "jsonrpc": "2.0",
                    "id": next(self._ids),
                    "method": "eth_getTransactionReceipt",
                    "params": [tx_hash],
                }
                for tx_hash in chunk
            ]
            for attempt in range(self._max_attempts):
                body = self._post(requests)
                if not isinstance(body, list):
                    raise RpcError("batch receipt request returned a non-list result: {body!r}")
                by_id = {item.get("id"): item for item in body if isinstance(item, dict)}
                retryable_error: RpcResponseError | None = None
                chunk_receipts: list[dict[str, Any]] = []
                for request, tx_hash in zip(requests, chunk, strict=True):
                    item = by_id.get(request["id"])
                    if item is None:
                        raise RpcError(f"batch response omitted receipt for {tx_hash}")
                    if "error" in item:
                        error = item["error"] if isinstance(item["error"], dict) else {}
                        rpc_error = RpcResponseError(
                            "eth_getTransactionReceipt",
                            error.get("code"),
                            error.get("message", "unknown error"),
                        )
                        code = rpc_error.code
                        if isinstance(code, int) and -32099 <= code <= -32000:
                            retryable_error = rpc_error
                            break
                        raise rpc_error
                    result = item.get("result")
                    if not isinstance(result, dict):
                        raise RpcNotFoundError(f"receipt for {tx_hash} was not found")
                    chunk_receipts.append(result)
                if retryable_error is None:
                    receipts.extend(chunk_receipts)
                    break
                if attempt + 1 == self._max_attempts:
                    raise retryable_error
                time.sleep(self._backoff_seconds * (2**attempt))
        return receipts

    def get_logs(
        self,
        *,
        from_block: int | BlockTag,
        to_block: int | BlockTag,
        address: str | None = None,
        topics: list[str | None] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "fromBlock": encode_block_identifier(from_block),
            "toBlock": encode_block_identifier(to_block),
        }
        if address is not None:
            params["address"] = address
        if topics is not None:
            params["topics"] = topics
        result = self.call("eth_getLogs", [params])
        if not isinstance(result, list) or any(not isinstance(log, dict) for log in result):
            raise RpcError("eth_getLogs returned a malformed result")
        return result
