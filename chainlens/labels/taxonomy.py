"""The deliberately small, versioned Milestone 4 label taxonomy."""

TAXONOMY_VERSION = "taxonomy_v1"

TAXONOMY: dict[str, frozenset[str]] = {
    "entity_category": frozenset(
        {"dex", "exchange", "bridge", "protocol", "token_issuer", "unknown"}
    ),
    "contract_role": frozenset(
        {
            "router",
            "factory",
            "pool_manager",
            "position_manager",
            "token_contract",
            "protocol_contract",
            "hot_wallet",
            "deposit_wallet",
            "deployer",
            "unknown",
        }
    ),
}


def validate_taxonomy_version(version: str) -> None:
    if version != TAXONOMY_VERSION:
        raise ValueError(f"unsupported taxonomy version: {version}")
