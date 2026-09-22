"""Focused Milestone 6 API tests without a live RPC or ClickHouse server."""

from datetime import UTC, datetime
from uuid import UUID

from fastapi.testclient import TestClient

from chainlens.api.app import create_app
from chainlens.api.resolvers import MAX_LIMIT, normalize_address


CLUSTER_RUN = UUID("11111111-1111-1111-1111-111111111111")
OLD_CLUSTER_RUN = UUID("22222222-2222-2222-2222-222222222222")
LABEL_RUN = UUID("33333333-3333-3333-3333-333333333333")
OLD_LABEL_RUN = UUID("44444444-4444-4444-4444-444444444444")
EVALUATION_RUN = UUID("55555555-5555-5555-5555-555555555555")
WALLET = "0x" + "ab" * 20
UNCLUSTERED = "0x" + "cd" * 20


class FakeStore:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.calls: list[tuple[str, object]] = []

    def close(self) -> None: pass
    def ping(self) -> None:
        if not self.ready:
            raise ConnectionError("offline")

    def current_cluster_run(self, chain_id):
        self.calls.append(("current_cluster_run", chain_id))
        return {"run_id": CLUSTER_RUN, "heuristic_version": "shared_funder_v1"}

    def cluster_run(self, run_id):
        if run_id == OLD_CLUSTER_RUN:
            return {"chain_id": 8453, "status": "completed", "heuristic_version": "shared_funder_v1"}
        return None

    def current_label_run(self, chain_id):
        self.calls.append(("current_label_run", chain_id))
        return {"run_id": LABEL_RUN, "cluster_run_id": CLUSTER_RUN, "taxonomy_version": "taxonomy_v1"}

    def label_run(self, run_id):
        if run_id == OLD_LABEL_RUN:
            return {"chain_id": 8453, "status": "completed", "cluster_run_id": OLD_CLUSTER_RUN, "taxonomy_version": "taxonomy_v1"}
        return None

    def current_evaluation_run(self, chain_id):
        return {"run_id": EVALUATION_RUN, "cluster_run_id": CLUSTER_RUN,
                "label_run_id": LABEL_RUN, "heuristic_version": "shared_funder_v1",
                "taxonomy_version": "taxonomy_v1", "ground_truth_version": "sha256:test",
                "evaluation_version": "evaluation_v1"}

    def evaluation_run(self, run_id): return None

    def wallet_features(self, chain_id, address, run_id, *, current):
        if address not in {WALLET, UNCLUSTERED}: return None
        return {"first_seen_block": 1, "last_seen_block": 2,
                "incoming_native_tx_count": 1, "outgoing_native_tx_count": 0,
                "incoming_erc20_transfer_count": 0, "outgoing_erc20_transfer_count": 0,
                "unique_counterparties": 1, "native_received_wei": str(2**255),
                "native_sent_wei": "0", "first_funder_address": None,
                "first_funding_block": None}

    def wallet_cluster(self, chain_id, address, run_id, *, current):
        self.calls.append(("wallet_cluster_run", run_id))
        if address != WALLET: return None
        return {"cluster_id": "cluster-1", "cluster_size": 2, "confidence": .9,
                "heuristic_version": "shared_funder_v1"}

    def entity_for_wallet(self, chain_id, address, run_id, *, current):
        if address != WALLET: return None
        return {"entity_id": "entity-1", "display_name": "Uniswap",
                "entity_category": "dex", "cluster_id": "cluster-1",
                "membership_method": "direct_seed", "membership_confidence": 1.0}

    def entity(self, chain_id, entity_id, run_id, *, current):
        if entity_id != "entity-1": return None
        return {"entity_id": entity_id, "display_name": "Uniswap",
                "entity_category": "dex", "cluster_id": "cluster-1"}

    def entity_members(self, chain_id, entity_id, run_id, *, current, limit, offset):
        return [{"wallet_address": WALLET, "membership_method": "direct_seed",
                 "membership_confidence": 1.0, "cluster_id": "cluster-1"}][offset:offset+limit]

    def entities(self, chain_id, run_id, *, current, category, limit, offset):
        return [self.entity(chain_id, "entity-1", run_id, current=current)][offset:offset+limit]

    def labels(self, chain_id, subject_type, subject_id, run_id, *, current):
        if subject_id not in {WALLET, "entity-1"}: return []
        return [{"label_dimension": "entity_category", "label_value": "dex",
                 "confidence": 1.0, "assignment_method": "direct_seed",
                 "source_type": "curated_csv", "source_reference": "base-known-v1",
                 "taxonomy_version": "taxonomy_v1"}]

    def label_evidence(self, chain_id, subject_type, subject_id, run_id, *, current):
        if subject_id not in {WALLET, "entity-1"}: return []
        return [{"label_dimension": "entity_category", "label_value": "dex",
                 "evidence_type": "curated_seed", "evidence_value": WALLET,
                 "weight": 1.0, "confidence_contribution": 1.0,
                 "source_type": "curated_csv", "source_reference": "base-known-v1"}]

    def cluster(self, chain_id, cluster_id, run_id, *, current):
        if cluster_id != "cluster-1": return None
        return {"cluster_id": cluster_id, "cluster_size": 2, "confidence": .9,
                "heuristic_version": "shared_funder_v1"}

    def cluster_members(self, chain_id, cluster_id, run_id, *, current, limit, offset):
        rows = [{"wallet_address": WALLET, "confidence": .9},
                {"wallet_address": "0x" + "ef" * 20, "confidence": .8}]
        return rows[offset:offset+limit]

    def clusters(self, chain_id, run_id, *, current, min_size, limit, offset):
        return [self.cluster(chain_id, "cluster-1", run_id, current=current)][offset:offset+limit]

    def cluster_edges(self, chain_id, cluster_id, run_id, *, current, limit, offset):
        return [{"wallet_a": WALLET, "wallet_b": "0x" + "ef" * 20,
                 "heuristic": "shared_funder", "score": .9, "evidence_count": 1}][offset:offset+limit]

    def cluster_evidence(self, chain_id, cluster_id, run_id, *, current, limit, offset):
        return [{"wallet_a": WALLET, "wallet_b": "0x" + "ef" * 20,
                 "heuristic": "shared_funder", "evidence_type": "shared_funder",
                 "evidence_value": "0x" + "11" * 20, "weight": 1.0,
                 "score_contribution": .9}][offset:offset+limit]

    def label_evaluations(self, chain_id, run_id, *, current):
        return [{"label_dimension": "entity_category", "label_value": "dex",
                 "assignment_method": "cluster_propagation", "tp": 0, "fp": 0,
                 "fn": 1, "precision": None, "recall": 0.0, "f1": None,
                 "support": 1, "predicted_count": 0, "coverage": 0.0}]

    def clustering_evaluations(self, chain_id, run_id, *, current):
        return [{"metric_scope": "curated_seen_addresses", "tp_pairs": 0,
                 "fp_pairs": 0, "fn_pairs": 0, "pairwise_precision": None,
                 "pairwise_recall": None, "pairwise_f1": None,
                 "ground_truth_entity_count": 1, "ground_truth_address_count": 1,
                 "evaluated_address_count": 0, "predicted_cluster_count": 1}]

    def drift_metrics(self, chain_id, run_id, *, current):
        return [{"metric_group": "coverage", "metric_name": "seed_seen_count",
                 "scope_key": "all", "value": 1.0, "previous_value": None,
                 "absolute_change": None, "relative_change": None}]

    def drift_alerts(self, chain_id, run_id, *, current, severity, limit):
        row = {"severity": "warning", "metric_name": "seed_coverage",
               "scope_key": "all", "current_value": .5, "previous_value": 1.0,
               "threshold_type": "absolute_drop", "threshold_value": .2,
               "message": "coverage dropped", "created_at": datetime.now(UTC)}
        return [] if severity and severity != "warning" else [row][:limit]


def graphql(client: TestClient, query: str) -> dict:
    return client.post("/graphql", json={"query": query}).json()


def test_wallet_address_normalization_and_invalid_rejection() -> None:
    assert normalize_address("0x" + "AB" * 20) == WALLET
    with TestClient(create_app(FakeStore())) as client:
        result = graphql(client, '{ wallet(address: "not-an-address") { address } }')
    assert "exactly 40 hex" in result["errors"][0]["message"]


def test_wallet_without_cluster_uses_null() -> None:
    with TestClient(create_app(FakeStore())) as client:
        data = graphql(client, f'{{ wallet(address: "{UNCLUSTERED}") {{ features {{ firstSeenBlock }} cluster {{ id }} entity {{ id }} }} }}')["data"]["wallet"]
    assert data == {"features": {"firstSeenBlock": 1}, "cluster": None, "entity": None}


def test_wallet_cluster_entity_label_provenance_and_uint256_string() -> None:
    query = f'''{{ wallet(address: "{WALLET.upper().replace("0X", "0x")}") {{
      address features {{ nativeReceivedWei }}
      entity {{ id membershipMethod membershipConfidence }}
      cluster {{ id size }}
      labels {{ dimension value assignmentMethod sourceType sourceReference taxonomyVersion
        evidence {{ type value weight confidenceContribution }} }}
    }} }}'''
    with TestClient(create_app(FakeStore())) as client:
        result = graphql(client, query)["data"]["wallet"]
    assert result["address"] == WALLET
    assert result["features"]["nativeReceivedWei"] == str(2**255)
    assert result["cluster"]["id"] == "cluster-1"
    assert result["entity"]["membershipMethod"] == "direct_seed"
    assert result["labels"][0]["sourceReference"] == "base-known-v1"
    assert result["labels"][0]["evidence"][0]["type"] == "curated_seed"


def test_cluster_evidence_and_entity_members() -> None:
    query = '''{ cluster(id: "cluster-1") { members(limit: 1) { address }
      resolutionEdges { heuristic evidenceCount } evidence { evidenceType evidenceValue } }
      entity(id: "entity-1") { members { address membershipMethod } } }'''
    with TestClient(create_app(FakeStore())) as client:
        data = graphql(client, query)["data"]
    assert len(data["cluster"]["members"]) == 1
    assert data["cluster"]["resolutionEdges"][0]["evidenceCount"] == 1
    assert data["cluster"]["evidence"][0]["evidenceType"] == "shared_funder"
    assert data["entity"]["members"][0]["address"] == WALLET


def test_current_snapshot_default_and_explicit_historical_runs() -> None:
    store = FakeStore()
    with TestClient(create_app(store)) as client:
        current = graphql(client, f'{{ wallet(address: "{WALLET}") {{ snapshot {{ clusterRunId labelRunId }} }} }}')
        historical = graphql(client, f'''{{ wallet(address: "{WALLET}",
          clusterRunId: "{OLD_CLUSTER_RUN}", labelRunId: "{OLD_LABEL_RUN}")
          {{ snapshot {{ clusterRunId labelRunId }} }} }}''')
    assert current["data"]["wallet"]["snapshot"]["clusterRunId"] == str(CLUSTER_RUN)
    assert historical["data"]["wallet"]["snapshot"]["labelRunId"] == str(OLD_LABEL_RUN)
    assert ("current_cluster_run", 8453) in store.calls
    assert ("wallet_cluster_run", OLD_CLUSTER_RUN) in store.calls


def test_invalid_run_fails_clearly() -> None:
    missing = UUID("99999999-9999-9999-9999-999999999999")
    with TestClient(create_app(FakeStore())) as client:
        result = graphql(client, f'{{ cluster(id: "x", clusterRunId: "{missing}") {{ id }} }}')
    assert f"cluster run {missing} does not exist" in result["errors"][0]["message"]


def test_null_metrics_remain_null_and_alerts_are_filterable() -> None:
    with TestClient(create_app(FakeStore())) as client:
        data = graphql(client, '''{ evaluation { labelMetrics { precision f1 }
          clusteringMetrics { pairwisePrecision pairwiseF1 } }
          driftAlerts(severity: "warning") { severity message } }''')["data"]
    assert data["evaluation"]["labelMetrics"][0] == {"precision": None, "f1": None}
    assert data["evaluation"]["clusteringMetrics"][0]["pairwiseF1"] is None
    assert data["driftAlerts"][0]["severity"] == "warning"


def test_pagination_maximum_is_rejected() -> None:
    with TestClient(create_app(FakeStore())) as client:
        result = graphql(client, f'{{ entities(limit: {MAX_LIMIT + 1}) {{ id }} }}')
    assert f"limit must be between 1 and {MAX_LIMIT}" in result["errors"][0]["message"]


def test_health_and_readiness() -> None:
    with TestClient(create_app(FakeStore())) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/readiness").json() == {"status": "ok"}
    with TestClient(create_app(FakeStore(ready=False))) as client:
        response = client.get("/readiness")
        assert response.status_code == 503
        assert response.json() == {"detail": "ClickHouse unavailable"}
