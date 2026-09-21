"""Pure deterministic derivation and graph functions."""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from hashlib import sha256
from itertools import combinations

from chainlens.clustering.models import (
    CanonicalTokenTransfer,
    CanonicalTransaction,
    ClusterEvidence,
    ClusterMembership,
    DerivedSnapshot,
    FundingEdge,
    ResolutionEdge,
    TransferEdge,
    WalletFeature,
)


ZERO_ADDRESS = "0x" + "0" * 40
HEURISTIC_VERSION = "shared_funder_v1"


def stable_id(*parts: object) -> str:
    """Hash an unambiguous length-prefixed encoding of the supplied parts."""

    digest = sha256()
    for part in parts:
        encoded = str(part).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def is_valid_wallet_address(address: str) -> bool:
    if len(address) != 42 or not address.startswith("0x"):
        return False
    try:
        int(address[2:], 16)
    except ValueError:
        return False
    return address.lower() != ZERO_ADDRESS


def canonical_pair(left: str, right: str) -> tuple[str, str]:
    """Return one stable representation for an unordered wallet pair."""

    normalized_left, normalized_right = left.lower(), right.lower()
    if normalized_left == normalized_right:
        raise ValueError("an entity-resolution edge requires two distinct wallets")
    if normalized_left < normalized_right:
        return normalized_left, normalized_right
    return normalized_right, normalized_left


@dataclass
class _FeatureState:
    blocks: list[int] = field(default_factory=list)
    in_native: int = 0
    out_native: int = 0
    in_erc20: int = 0
    out_erc20: int = 0
    received: int = 0
    sent: int = 0
    counterparties: set[str] = field(default_factory=set)


def derive_transfer_edges(
    transactions: Iterable[CanonicalTransaction],
    token_transfers: Iterable[CanonicalTokenTransfer],
) -> list[TransferEdge]:
    """Turn successful canonical facts into useful wallet interaction edges."""

    edges: list[TransferEdge] = []
    for tx in transactions:
        sender = tx.from_address.lower()
        recipient = tx.to_address.lower() if tx.to_address else None
        if (
            tx.status == 0
            or tx.value_wei <= 0
            or recipient is None
            or sender == recipient
            or not is_valid_wallet_address(sender)
            or not is_valid_wallet_address(recipient)
        ):
            continue
        edges.append(
            TransferEdge(
                tx.chain_id,
                tx.block_number,
                tx.block_hash,
                tx.block_timestamp,
                tx.tx_hash,
                tx.tx_index,
                "native",
                None,
                sender,
                recipient,
                tx.value_wei,
                stable_id(tx.chain_id, tx.tx_hash.lower(), "native"),
            )
        )
    for transfer in token_transfers:
        sender = transfer.from_address.lower()
        recipient = transfer.to_address.lower()
        if (
            transfer.amount_raw <= 0
            or sender == recipient
            or not is_valid_wallet_address(sender)
            or not is_valid_wallet_address(recipient)
        ):
            continue
        edges.append(
            TransferEdge(
                transfer.chain_id,
                transfer.block_number,
                transfer.block_hash,
                transfer.block_timestamp,
                transfer.tx_hash,
                transfer.log_index,
                "erc20",
                transfer.token_address.lower(),
                sender,
                recipient,
                transfer.amount_raw,
                stable_id(
                    transfer.chain_id,
                    transfer.tx_hash.lower(),
                    transfer.log_index,
                ),
            )
        )
    return sorted(edges, key=lambda edge: (edge.block_number, edge.tx_index, edge.edge_id))


def derive_first_funders(edges: Iterable[TransferEdge]) -> list[FundingEdge]:
    """Select the earliest native inbound edge per funded wallet."""

    native = sorted(
        (edge for edge in edges if edge.asset_type == "native" and edge.amount_raw > 0),
        key=lambda edge: (edge.block_number, edge.tx_index, edge.tx_hash, edge.edge_id),
    )
    first: dict[tuple[int, str], TransferEdge] = {}
    for edge in native:
        first.setdefault((edge.chain_id, edge.to_address), edge)
    return [
        FundingEdge(
            chain_id=edge.chain_id,
            funded_address=edge.to_address,
            funder_address=edge.from_address,
            funding_tx_hash=edge.tx_hash,
            funding_block_number=edge.block_number,
            funding_block_hash=edge.block_hash,
            funding_timestamp=edge.block_timestamp,
            amount_wei=edge.amount_raw,
        )
        for edge in sorted(first.values(), key=lambda item: (item.chain_id, item.to_address))
    ]


def derive_wallet_features(
    edges: Iterable[TransferEdge], funding_edges: Iterable[FundingEdge]
) -> list[WalletFeature]:
    """Aggregate a deliberately small feature set over the bounded snapshot."""

    edge_list = list(edges)
    funders = {(edge.chain_id, edge.funded_address): edge for edge in funding_edges}
    state: dict[tuple[int, str], _FeatureState] = {}

    def wallet_state(chain_id: int, wallet: str) -> _FeatureState:
        return state.setdefault((chain_id, wallet), _FeatureState())

    for edge in edge_list:
        sender = wallet_state(edge.chain_id, edge.from_address)
        recipient = wallet_state(edge.chain_id, edge.to_address)
        sender.blocks.append(edge.block_number)
        recipient.blocks.append(edge.block_number)
        sender.counterparties.add(edge.to_address)
        recipient.counterparties.add(edge.from_address)
        if edge.asset_type == "native":
            sender.out_native += 1
            recipient.in_native += 1
            sender.sent += edge.amount_raw
            recipient.received += edge.amount_raw
        else:
            sender.out_erc20 += 1
            recipient.in_erc20 += 1

    features: list[WalletFeature] = []
    for (chain_id, wallet), values in sorted(state.items()):
        funding = funders.get((chain_id, wallet))
        features.append(
            WalletFeature(
                chain_id,
                wallet,
                min(values.blocks),
                max(values.blocks),
                values.in_native,
                values.out_native,
                values.in_erc20,
                values.out_erc20,
                len(values.counterparties),
                values.received,
                values.sent,
                funding.funder_address if funding else None,
                funding.funding_block_number if funding else None,
            )
        )
    return features


def shared_funder_score(
    delta_seconds: int,
    fanout: int,
    window_seconds: int,
    max_fanout: int,
) -> tuple[float, float, float]:
    """Return final score plus transparent time and low-fanout contributions."""

    time_bonus = 0.25 * max(0.0, 1.0 - (delta_seconds / window_seconds))
    if max_fanout <= 2:
        fanout_bonus = 0.15
    else:
        fanout_bonus = 0.15 * max(0.0, 1.0 - ((fanout - 2) / (max_fanout - 2)))
    return min(1.0, 0.60 + time_bonus + fanout_bonus), time_bonus, fanout_bonus


def derive_shared_funder_edges(
    funding_edges: Iterable[FundingEdge],
    *,
    window_seconds: int,
    max_fanout: int,
) -> tuple[list[ResolutionEdge], list[ClusterEvidence]]:
    if window_seconds <= 0 or max_fanout <= 0:
        raise ValueError("shared-funder guards must be positive")
    groups: dict[tuple[int, str], list[FundingEdge]] = defaultdict(list)
    for edge in funding_edges:
        if is_valid_wallet_address(edge.funder_address) and is_valid_wallet_address(
            edge.funded_address
        ):
            groups[(edge.chain_id, edge.funder_address)].append(edge)

    resolution: list[ResolutionEdge] = []
    evidence: list[ClusterEvidence] = []
    for (chain_id, funder), funded in sorted(groups.items()):
        unique = {edge.funded_address: edge for edge in funded}
        fanout = len(unique)
        if fanout < 2 or fanout > max_fanout:
            continue
        ordered_funding = sorted(
            unique.values(), key=lambda edge: edge.funded_address
        )
        for left, right in combinations(ordered_funding, 2):
            delta = abs(
                int(
                    (left.funding_timestamp - right.funding_timestamp).total_seconds()
                )
            )
            if delta > window_seconds:
                continue
            wallet_a, wallet_b = canonical_pair(left.funded_address, right.funded_address)
            score, time_bonus, fanout_bonus = shared_funder_score(
                delta, fanout, window_seconds, max_fanout
            )
            resolution.append(
                ResolutionEdge(
                    chain_id,
                    wallet_a,
                    wallet_b,
                    "shared_funder",
                    score,
                    3,
                    stable_id(chain_id, "shared_funder", wallet_a, wallet_b),
                )
            )
            common = (chain_id, wallet_a, wallet_b, "shared_funder")
            evidence.extend(
                [
                    ClusterEvidence(*common, "same_first_funder", funder, 0.60, 0.60),
                    ClusterEvidence(*common, "funding_time_delta_seconds", str(delta), 0.25, time_bonus),
                    ClusterEvidence(*common, "funder_fanout", str(fanout), 0.15, fanout_bonus),
                ]
            )
    return resolution, evidence


def connected_components(edges: Iterable[ResolutionEdge]) -> list[tuple[str, ...]]:
    """Compute components only from explicit entity-resolution edges."""

    parent: dict[str, str] = {}

    def find(wallet: str) -> str:
        parent.setdefault(wallet, wallet)
        while parent[wallet] != wallet:
            parent[wallet] = parent[parent[wallet]]
            wallet = parent[wallet]
        return wallet

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            smaller, larger = sorted((left_root, right_root))
            parent[larger] = smaller

    for edge in edges:
        union(edge.wallet_a, edge.wallet_b)
    groups: dict[str, list[str]] = defaultdict(list)
    for wallet in sorted(parent):
        groups[find(wallet)].append(wallet)
    return sorted(tuple(sorted(members)) for members in groups.values() if len(members) > 1)


def derive_clusters(
    chain_id: int, edges: Iterable[ResolutionEdge]
) -> list[ClusterMembership]:
    """Create deterministic non-singleton clusters with minimum-edge confidence."""

    edge_list = list(edges)
    memberships: list[ClusterMembership] = []
    for members in connected_components(edge_list):
        member_set = set(members)
        scores = [
            edge.score
            for edge in edge_list
            if edge.wallet_a in member_set and edge.wallet_b in member_set
        ]
        confidence = min(scores)
        cluster_id = stable_id(chain_id, "cluster", *members)
        memberships.extend(
            ClusterMembership(chain_id, cluster_id, wallet, len(members), confidence)
            for wallet in members
        )
    return memberships


def derive_snapshot(
    chain_id: int,
    transactions: Iterable[CanonicalTransaction],
    token_transfers: Iterable[CanonicalTokenTransfer],
    *,
    window_seconds: int,
    max_fanout: int,
) -> DerivedSnapshot:
    transfer_edges = derive_transfer_edges(transactions, token_transfers)
    funding_edges = derive_first_funders(transfer_edges)
    wallet_features = derive_wallet_features(transfer_edges, funding_edges)
    resolution_edges, evidence = derive_shared_funder_edges(
        funding_edges, window_seconds=window_seconds, max_fanout=max_fanout
    )
    clusters = derive_clusters(chain_id, resolution_edges)
    return DerivedSnapshot(
        tuple(transfer_edges),
        tuple(funding_edges),
        tuple(wallet_features),
        tuple(resolution_edges),
        tuple(evidence),
        tuple(clusters),
    )
