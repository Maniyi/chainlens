"""Focused deterministic tests for Milestone 5 evaluation and drift logic."""

from chainlens.evaluation.logic import (
    build_drift_metrics,
    compare_drift_metrics,
    evaluate_clustering,
    evaluate_labels,
    generate_drift_alerts,
    precision_recall_f1,
)
from chainlens.evaluation.models import DriftMetric, DriftThresholds, StoredLabelAssignment
from chainlens.labels.models import SeedLabel


CHAIN_ID = 8453


def address(value: int) -> str:
    return f"0x{value:040x}"


def seed(value: int, entity: str, category: str = "dex", role: str | None = "router") -> SeedLabel:
    return SeedLabel(
        CHAIN_ID, address(value), entity, category, role, "test", f"ref-{value}", 1.0, ""
    )


def assignment(
    value: int,
    dimension: str,
    label: str,
    method: str,
    *,
    confidence: float = 1.0,
) -> StoredLabelAssignment:
    return StoredLabelAssignment(
        "address", address(value), None, dimension, label, confidence, method
    )


def test_precision_recall_f1_perfect_predictions() -> None:
    values = precision_recall_f1(4, 0, 0)
    assert (values.precision, values.recall, values.f1) == (1.0, 1.0, 1.0)


def test_precision_recall_f1_undefined_cases() -> None:
    no_predictions = precision_recall_f1(0, 0, 3)
    assert no_predictions.precision is None
    assert no_predictions.recall == 0.0
    assert no_predictions.f1 is None

    no_positives = precision_recall_f1(0, 2, 0)
    assert no_positives.precision == 0.0
    assert no_positives.recall is None
    assert no_positives.f1 is None


def test_precision_recall_f1_false_positive_and_false_negative() -> None:
    values = precision_recall_f1(2, 1, 1)
    assert values.precision == 2 / 3
    assert values.recall == 2 / 3
    assert values.f1 == 2 / 3


def test_pairwise_clustering_perfect_case() -> None:
    seeds = [seed(1, "A"), seed(2, "A"), seed(3, "B"), seed(4, "B")]
    clusters = {address(1): "one", address(2): "one", address(3): "two", address(4): "two"}
    result = evaluate_clustering(seeds, clusters, clusters)
    assert (result.tp_pairs, result.fp_pairs, result.fn_pairs) == (2, 0, 0)
    assert (result.pairwise_precision, result.pairwise_recall, result.pairwise_f1) == (1.0, 1.0, 1.0)


def test_pairwise_clustering_false_positive() -> None:
    seeds = [seed(1, "A"), seed(2, "B")]
    result = evaluate_clustering(
        seeds, [address(1), address(2)], {address(1): "mixed", address(2): "mixed"}
    )
    assert (result.tp_pairs, result.fp_pairs, result.fn_pairs) == (0, 1, 0)
    assert result.pairwise_precision == 0.0
    assert result.pairwise_recall is None


def test_pairwise_clustering_false_negative_for_unclustered_address() -> None:
    seeds = [seed(1, "A"), seed(2, "A")]
    result = evaluate_clustering(
        seeds, [address(1), address(2)], {address(1): "one"}
    )
    assert (result.tp_pairs, result.fp_pairs, result.fn_pairs) == (0, 0, 1)
    assert result.pairwise_precision is None
    assert result.pairwise_recall == 0.0


def test_pairwise_unknown_ground_truth_is_excluded() -> None:
    seeds = [seed(1, "A"), seed(2, "A")]
    result = evaluate_clustering(
        seeds,
        [address(1), address(2), address(99)],
        {address(1): "one", address(2): "one", address(99): "two"},
    )
    assert result.evaluated_address_count == 2
    assert (result.tp_pairs, result.fp_pairs, result.fn_pairs) == (1, 0, 0)


def test_direct_label_evaluation_reproduces_curated_truth() -> None:
    seeds = [seed(1, "A", role="router"), seed(2, "A", role="factory")]
    predictions = [
        assignment(1, "entity_category", "dex", "direct_seed"),
        assignment(2, "entity_category", "dex", "direct_seed"),
        assignment(1, "contract_role", "router", "direct_seed"),
        assignment(2, "contract_role", "factory", "direct_seed"),
    ]
    rows = evaluate_labels(seeds, predictions)
    direct = [row for row in rows if row.assignment_method == "direct_seed"]
    assert all(row.fp == 0 and row.fn == 0 and row.precision == 1 for row in direct)
    assert all(row.coverage == 1 for row in direct)


def test_propagated_label_evaluation_has_tp_fp_and_fn() -> None:
    seeds = [seed(1, "A", "dex"), seed(2, "B", "protocol"), seed(3, "C", "dex")]
    predictions = [
        assignment(1, "entity_category", "dex", "cluster_propagation"),
        assignment(2, "entity_category", "dex", "cluster_propagation"),
    ]
    dex = next(
        row for row in evaluate_labels(seeds, predictions)
        if row.assignment_method == "cluster_propagation"
        and row.label_dimension == "entity_category"
        and row.label_value == "dex"
    )
    assert (dex.tp, dex.fp, dex.fn) == (1, 1, 1)
    assert (dex.precision, dex.recall, dex.f1) == (0.5, 0.5, 0.5)
    assert dex.coverage == 2 / 3


def test_no_propagated_labels_has_null_precision_and_zero_recall() -> None:
    row = next(
        item for item in evaluate_labels([seed(1, "A")], [])
        if item.assignment_method == "cluster_propagation"
        and item.label_dimension == "entity_category"
    )
    assert row.predicted_count == 0
    assert row.precision is None
    assert row.recall == 0.0
    assert row.f1 is None


def test_coverage_metrics_are_counts_not_accuracy() -> None:
    metrics = build_drift_metrics(
        wallet_count=2,
        resolution_edge_count=1,
        cluster_by_address={address(1): "one", address(9): "one"},
        cluster_confidence_by_address={address(1): 0.8, address(9): 0.8},
        entity_count=1,
        assignments=[
            assignment(1, "entity_category", "dex", "direct_seed"),
            assignment(2, "entity_category", "dex", "cluster_propagation"),
        ],
        conflict_count=0,
        seeds=[seed(1, "A"), seed(2, "A"), seed(3, "B")],
        seen_addresses=[address(1), address(2)],
    )
    values = {metric.metric_name: metric.value for metric in metrics if metric.metric_group == "coverage"}
    assert values == {
        "seed_count": 3.0,
        "seed_seen_count": 2.0,
        "seed_cluster_overlap_count": 1.0,
        "ground_truth_addresses_directly_labeled": 1.0,
        "ground_truth_addresses_with_inferred_labels": 1.0,
    }


def test_drift_largest_cluster_increase_alert() -> None:
    metric = compare_drift_metrics(
        [DriftMetric("clustering", "largest_cluster_size", "all", 25.0)],
        {("clustering", "largest_cluster_size", "all"): 10.0},
    )[0]
    alerts = generate_drift_alerts([metric], DriftThresholds())
    assert [(alert.metric_name, alert.severity) for alert in alerts] == [
        ("largest_cluster_size", "warning")
    ]


def test_drift_cluster_count_drop_requires_stable_wallet_count() -> None:
    metrics = compare_drift_metrics(
        [
            DriftMetric("clustering", "cluster_count", "all", 4.0),
            DriftMetric("clustering", "wallet_count", "all", 102.0),
        ],
        {
            ("clustering", "cluster_count", "all"): 10.0,
            ("clustering", "wallet_count", "all"): 100.0,
        },
    )
    alerts = generate_drift_alerts(metrics, DriftThresholds())
    assert [alert.metric_name for alert in alerts] == ["cluster_count"]
