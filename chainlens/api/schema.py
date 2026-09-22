"""Strawberry GraphQL query schema."""

from uuid import UUID

import strawberry
from strawberry.types import Info

from chainlens.api.resolvers import APIResolver
from chainlens.api.types import Cluster, DriftAlert, Entity, EvaluationSummary, Wallet


def _resolver(info: Info) -> APIResolver:
    return info.context["resolver"]


@strawberry.type
class Query:
    @strawberry.field
    def wallet(
        self, info: Info, address: str, chain_id: int = 8453,
        cluster_run_id: UUID | None = None, label_run_id: UUID | None = None,
    ) -> Wallet:
        return _resolver(info).wallet(address, chain_id, cluster_run_id, label_run_id)

    @strawberry.field
    def entity(
        self, info: Info, id: str, chain_id: int = 8453,
        label_run_id: UUID | None = None,
    ) -> Entity | None:
        return _resolver(info).entity(id, chain_id, label_run_id)

    @strawberry.field
    def entities(
        self, info: Info, chain_id: int = 8453, category: str | None = None,
        limit: int = 50, offset: int = 0, label_run_id: UUID | None = None,
    ) -> list[Entity]:
        return _resolver(info).entities(
            chain_id, category, limit, offset, label_run_id
        )

    @strawberry.field
    def cluster(
        self, info: Info, id: str, chain_id: int = 8453,
        cluster_run_id: UUID | None = None,
    ) -> Cluster | None:
        return _resolver(info).cluster(id, chain_id, cluster_run_id)

    @strawberry.field
    def clusters(
        self, info: Info, chain_id: int = 8453, min_size: int | None = None,
        limit: int = 50, offset: int = 0, cluster_run_id: UUID | None = None,
    ) -> list[Cluster]:
        return _resolver(info).clusters(
            chain_id, min_size, limit, offset, cluster_run_id
        )

    @strawberry.field
    def evaluation(
        self, info: Info, chain_id: int = 8453,
        evaluation_run_id: UUID | None = None,
    ) -> EvaluationSummary | None:
        return _resolver(info).evaluation(chain_id, evaluation_run_id)

    @strawberry.field
    def drift_alerts(
        self, info: Info, chain_id: int = 8453, severity: str | None = None,
        limit: int = 50, evaluation_run_id: UUID | None = None,
    ) -> list[DriftAlert]:
        return _resolver(info).drift_alerts(
            chain_id, severity, limit, evaluation_run_id
        )


schema = strawberry.Schema(query=Query)
