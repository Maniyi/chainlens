"""Small data models shared by clustering derivation and persistence."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class CanonicalTransaction:
    chain_id: int
    block_number: int
    block_hash: str
    block_timestamp: datetime
    tx_hash: str
    tx_index: int
    from_address: str
    to_address: str | None
    value_wei: int
    status: int | None = 1


@dataclass(frozen=True)
class CanonicalTokenTransfer:
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


@dataclass(frozen=True)
class TransferEdge:
    chain_id: int
    block_number: int
    block_hash: str
    block_timestamp: datetime
    tx_hash: str
    tx_index: int
    asset_type: str
    token_address: str | None
    from_address: str
    to_address: str
    amount_raw: int
    edge_id: str


@dataclass(frozen=True)
class FundingEdge:
    chain_id: int
    funded_address: str
    funder_address: str
    funding_tx_hash: str
    funding_block_number: int
    funding_block_hash: str
    funding_timestamp: datetime
    amount_wei: int
    heuristic: str = "first_native_funder"
    confidence: float = 0.7


@dataclass(frozen=True)
class WalletFeature:
    chain_id: int
    wallet_address: str
    first_seen_block: int
    last_seen_block: int
    incoming_native_tx_count: int
    outgoing_native_tx_count: int
    incoming_erc20_transfer_count: int
    outgoing_erc20_transfer_count: int
    unique_counterparties: int
    native_received_wei: int
    native_sent_wei: int
    first_funder_address: str | None
    first_funding_block: int | None


@dataclass(frozen=True)
class ResolutionEdge:
    chain_id: int
    wallet_a: str
    wallet_b: str
    heuristic: str
    score: float
    evidence_count: int
    edge_id: str


@dataclass(frozen=True)
class ClusterEvidence:
    chain_id: int
    wallet_a: str
    wallet_b: str
    heuristic: str
    evidence_type: str
    evidence_value: str
    weight: float
    score_contribution: float


@dataclass(frozen=True)
class ClusterMembership:
    chain_id: int
    cluster_id: str
    wallet_address: str
    cluster_size: int
    confidence: float


@dataclass(frozen=True)
class DerivedSnapshot:
    transfer_edges: tuple[TransferEdge, ...]
    funding_edges: tuple[FundingEdge, ...]
    wallet_features: tuple[WalletFeature, ...]
    resolution_edges: tuple[ResolutionEdge, ...]
    evidence: tuple[ClusterEvidence, ...]
    clusters: tuple[ClusterMembership, ...]


@dataclass(frozen=True)
class ClusterRunSummary:
    run_id: str
    chain_id: int
    start_block: int
    end_block: int
    transfer_edge_count: int
    funding_edge_count: int
    wallet_feature_count: int
    resolution_edge_count: int
    cluster_count: int
    largest_cluster_size: int


@dataclass(frozen=True)
class ClusterRun:
    run_id: UUID
    chain_id: int
    start_block: int
    end_block: int
    heuristic_version: str
    status: str
    started_at: datetime
    version: int
