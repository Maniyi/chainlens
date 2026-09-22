"""FastAPI application and ``python -m`` entrypoint."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from strawberry.fastapi import GraphQLRouter

from chainlens.api.resolvers import APIResolver
from chainlens.api.schema import schema
from chainlens.api.store import ClickHouseAPIStore
from chainlens.db import create_client


def create_app(store: ClickHouseAPIStore | None = None) -> FastAPI:
    supplied_store = store

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        api_store = supplied_store or ClickHouseAPIStore(create_client())
        application.state.api_store = api_store
        application.state.resolver = APIResolver(api_store)
        try:
            yield
        finally:
            if supplied_store is None:
                api_store.close()

    application = FastAPI(title="ChainLens API", version="0.1.0", lifespan=lifespan)

    async def context_getter(request: Request) -> dict[str, object]:
        return {"request": request, "resolver": request.app.state.resolver}

    application.include_router(
        GraphQLRouter(schema, context_getter=context_getter), prefix="/graphql"
    )

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readiness")
    def readiness(request: Request) -> dict[str, str]:
        try:
            request.app.state.api_store.ping()
        except Exception as exc:
            raise HTTPException(status_code=503, detail="ClickHouse unavailable") from exc
        return {"status": "ok"}

    return application


app = create_app()


def main() -> None:
    uvicorn.run("chainlens.api.app:app", host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
