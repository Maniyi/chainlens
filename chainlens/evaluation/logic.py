"""Pure, deterministic evaluation and operational drift logic."""

from collections import Counter
from collections.abc import Iterable, Mapping
from itertools import combinations
from statistics import median

from chainlens.evaluation.models import (
    ClusterEvaluation,
    DriftAlert,
    DriftMetric,
    DriftThresholds,
    EvaluatedLabel,
    MetricValues,
    StoredLabelAssignment,
)
from chainlens.labels.logic import normalize_entity_name
from chainlens.labels.models import SeedLabel


EVALUATION_VERSION = "evaluation_v1"
LABEL_DIMENSIONS = ("entity_category", "contract_role")
ASSIGNMENT_METHODS = ("direct_seed", "cluster_propagation")


def precision_recall_f1(tp: int, fp: int, fn: int) -> MetricValues:
    """Return metrics with ``None`` whenever the relevant denominator is zero."""

    if min(tp, fp, fn) < 0:
        raise ValueError("metric counts cannot be negative")
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )
    return MetricValues(precision, recall, f1)


def _truth_by_dimension(
    seeds: Iterable[SeedLabel], dimension: str
) -> dict[str, str]:
    truth: dict[str, str] = {}
    for seed in seeds:
        value = (
            seed.entity_category
            if dimension == "entity_category"
            else seed.contract_role
        )
        if value is not None:
            truth[seed.address.lower()] = value
    return truth


def evaluate_labels(
    seeds: Iterable[SeedLabel], assignments: Iterable[StoredLabelAssignment]
) -> list[EvaluatedLabel]:
    """Evaluate address predictions one-vs-rest, separated by method and dimension.

    Predictions for subjects without curated truth contribute to ``predicted_count``
    but are not called false positives. This keeps precision scoped to cases whose
    correctness is actually knowable.
    """

    seed_list = list(seeds)
    assignment_list = [item for item in assignments if item.subject_type == "address"]
    output: list[EvaluatedLabel] = []
    for dimension in LABEL_DIMENSIONS:
        truth = _truth_by_dimension(seed_list, dimension)
        for method in ASSIGNMENT_METHODS:
            predictions = [
                item
                for item in assignment_list
                if item.label_dimension == dimension
                and item.assignment_method == method
            ]
            prediction_by_subject = {
                item.subject_id.lower(): item.label_value for item in predictions
            }
            values = sorted(
                set(truth.values())
                | {
                    item.label_value
                    for item in predictions
                    if item.subject_id.lower() in truth
                }
            )
            covered = len(set(truth) & set(prediction_by_subject))
            coverage = covered / len(truth) if truth else 0.0
            for value in values:
                positive_subjects = {
                    subject for subject, expected in truth.items() if expected == value
                }
                predicted_subjects = {
                    subject
                    for subject, predicted in prediction_by_subject.items()
                    if predicted == value
                }
                known_predictions = predicted_subjects & set(truth)
                tp = len(known_predictions & positive_subjects)
                fp = len(known_predictions - positive_subjects)
                fn = len(positive_subjects - predicted_subjects)
                metrics = precision_recall_f1(tp, fp, fn)
                output.append(
                    EvaluatedLabel(
                        dimension,
                        value,
                        method,
                        tp,
                        fp,
                        fn,
                        metrics.precision,
                        metrics.recall,
                        metrics.f1,
                        len(positive_subjects),
                        sum(item.label_value == value for item in predictions),
                        coverage,
                    )
                )
    return output


def evaluate_clustering(
    seeds: Iterable[SeedLabel],
    seen_addresses: Iterable[str],
    cluster_by_address: Mapping[str, str],
) -> ClusterEvaluation:
    """Compute pairwise metrics over curated addresses present in the run scope."""

    truth = {
        seed.address.lower(): normalize_entity_name(seed.entity_name) for seed in seeds
    }
    seen = {address.lower() for address in seen_addresses}
    evaluated = sorted(set(truth) & seen)
    predicted = {address.lower(): cluster for address, cluster in cluster_by_address.items()}
    tp = fp = fn = 0
    for left, right in combinations(evaluated, 2):
        same_truth = truth[left] == truth[right]
        same_cluster = (
            left in predicted
            and right in predicted
            and predicted[left] == predicted[right]
        )
        if same_cluster and same_truth:
            tp += 1
        elif same_cluster:
            fp += 1
        elif same_truth:
            fn += 1
    metrics = precision_recall_f1(tp, fp, fn)
    return ClusterEvaluation(
        "curated_entities_seen_in_cluster_scope",
        tp,
        fp,
        fn,
        metrics.precision,
        metrics.recall,
        metrics.f1,
        len({truth[address] for address in evaluated}),
        len(truth),
        len(evaluated),
        len({predicted[address] for address in evaluated if address in predicted}),
    )


def build_drift_metrics(
    *,
    wallet_count: int,
    resolution_edge_count: int,
    cluster_by_address: Mapping[str, str],
    cluster_confidence_by_address: Mapping[str, float],
    entity_count: int,
    assignments: Iterable[StoredLabelAssignment],
    conflict_count: int,
    seeds: Iterable[SeedLabel],
    seen_addresses: Iterable[str],
) -> list[DriftMetric]:
    """Build descriptive snapshot and coverage statistics without accuracy claims."""

    assignment_list = list(assignments)
    seed_list = list(seeds)
    seen = {value.lower() for value in seen_addresses}
    clusters = Counter(cluster_by_address.values())
    sizes = sorted(clusters.values())

    def percentile_95(values: list[int]) -> float:
        if not values:
            return 0.0
        index = max(0, (95 * len(values) + 99) // 100 - 1)
        return float(values[index])

    cluster_confidences: dict[str, float] = {}
    for address, cluster_id in cluster_by_address.items():
        confidence = cluster_confidence_by_address.get(address)
        if confidence is not None:
            cluster_confidences.setdefault(cluster_id, confidence)
    direct = [item for item in assignment_list if item.assignment_method == "direct_seed"]
    propagated = [
        item for item in assignment_list if item.assignment_method == "cluster_propagation"
    ]
    seed_addresses = {seed.address.lower() for seed in seed_list}
    clustered = set(cluster_by_address)
    direct_addresses = {
        item.subject_id.lower() for item in direct if item.subject_type == "address"
    }
    inferred_addresses = {
        item.subject_id.lower()
        for item in propagated
        if item.subject_type == "address"
    }
    metrics = [
        DriftMetric("clustering", "wallet_count", "all", float(wallet_count)),
        DriftMetric("clustering", "resolution_edge_count", "all", float(resolution_edge_count)),
        DriftMetric("clustering", "cluster_count", "all", float(len(clusters))),
        DriftMetric("clustering", "largest_cluster_size", "all", float(max(sizes, default=0))),
        DriftMetric("clustering", "average_cluster_size", "all", sum(sizes) / len(sizes) if sizes else 0.0),
        DriftMetric("clustering", "median_cluster_size", "all", float(median(sizes)) if sizes else 0.0),
        DriftMetric("clustering", "p95_cluster_size", "all", percentile_95(sizes)),
        DriftMetric("clustering", "average_cluster_confidence", "all", sum(cluster_confidences.values()) / len(cluster_confidences) if cluster_confidences else 0.0),
        DriftMetric("labels", "entity_count", "all", float(entity_count)),
        DriftMetric("labels", "direct_label_count", "all", float(len(direct))),
        DriftMetric("labels", "propagated_label_count", "all", float(len(propagated))),
        DriftMetric("labels", "conflict_count", "all", float(conflict_count)),
        DriftMetric("labels", "average_propagated_confidence", "all", sum(item.confidence for item in propagated) / len(propagated) if propagated else 0.0),
        DriftMetric("coverage", "seed_count", "all", float(len(seed_addresses))),
        DriftMetric("coverage", "seed_seen_count", "all", float(len(seed_addresses & seen))),
        DriftMetric("coverage", "seed_cluster_overlap_count", "all", float(len(seed_addresses & clustered))),
        DriftMetric("coverage", "ground_truth_addresses_directly_labeled", "all", float(len(seed_addresses & direct_addresses))),
        DriftMetric("coverage", "ground_truth_addresses_with_inferred_labels", "all", float(len(seed_addresses & inferred_addresses))),
    ]
    distributions = Counter(
        (item.label_dimension, item.label_value) for item in assignment_list
    )
    metrics.extend(
        DriftMetric("labels", "label_count", f"{dimension}={value}", float(count))
        for (dimension, value), count in sorted(distributions.items())
    )
    return metrics


def compare_drift_metrics(
    current: Iterable[DriftMetric], previous: Mapping[tuple[str, str, str], float]
) -> list[DriftMetric]:
    compared: list[DriftMetric] = []
    for metric in current:
        prior = previous.get((metric.metric_group, metric.metric_name, metric.scope_key))
        absolute = metric.value - prior if prior is not None else None
        relative = absolute / abs(prior) if prior not in (None, 0.0) else None
        compared.append(
            DriftMetric(
                metric.metric_group,
                metric.metric_name,
                metric.scope_key,
                metric.value,
                prior,
                absolute,
                relative,
            )
        )
    return compared


def generate_drift_alerts(
    metrics: Iterable[DriftMetric], thresholds: DriftThresholds
) -> list[DriftAlert]:
    """Apply a deliberately small set of deterministic operational guardrails."""

    indexed = {(m.metric_name, m.scope_key): m for m in metrics}
    alerts: list[DriftAlert] = []

    def add(metric: DriftMetric, severity: str, kind: str, threshold: float, message: str) -> None:
        alerts.append(
            DriftAlert(metric.metric_name, metric.scope_key, severity, metric.value,
                       metric.previous_value, kind, threshold, message)
        )

    largest = indexed.get(("largest_cluster_size", "all"))
    if largest and largest.relative_change is not None and largest.relative_change >= thresholds.largest_cluster_relative_increase:
        severity = "critical" if largest.relative_change >= 2 * thresholds.largest_cluster_relative_increase else "warning"
        add(largest, severity, "relative_increase", thresholds.largest_cluster_relative_increase,
            "Largest cluster size increased beyond the configured guardrail.")

    cluster_count = indexed.get(("cluster_count", "all"))
    wallets = indexed.get(("wallet_count", "all"))
    if (
        cluster_count and wallets and cluster_count.relative_change is not None
        and cluster_count.relative_change <= -thresholds.cluster_count_drop
        and (wallets.relative_change is None or abs(wallets.relative_change) <= 0.1)
    ):
        add(cluster_count, "warning", "relative_drop", thresholds.cluster_count_drop,
            "Cluster count dropped sharply while wallet count remained similar.")

    propagated = indexed.get(("propagated_label_count", "all"))
    if propagated and propagated.previous_value is not None:
        spike = (
            propagated.relative_change is not None
            and propagated.relative_change >= thresholds.propagated_label_spike
        ) or (propagated.previous_value == 0 and propagated.value > 0)
        if spike:
            add(propagated, "warning", "relative_increase", thresholds.propagated_label_spike,
                "Propagated label count increased beyond the configured guardrail.")

    conflicts = indexed.get(("conflict_count", "all"))
    if conflicts and conflicts.previous_value is not None and conflicts.value >= thresholds.conflict_count and conflicts.value > conflicts.previous_value:
        add(conflicts, "warning", "absolute_count", float(thresholds.conflict_count),
            "Label conflicts increased to a non-trivial count.")

    for name in ("seed_seen_count", "seed_cluster_overlap_count"):
        coverage = indexed.get((name, "all"))
        if coverage and coverage.relative_change is not None and coverage.relative_change <= -thresholds.seed_coverage_drop:
            add(coverage, "warning", "relative_drop", thresholds.seed_coverage_drop,
                f"{name} dropped beyond the configured guardrail.")
    return alerts
