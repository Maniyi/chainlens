"""Command-line ClickHouse health check."""

from chainlens.db import create_client


def main() -> int:
    """Verify that ClickHouse accepts a query."""

    client = create_client()
    try:
        result = client.query("SELECT 1")
        if result.first_row[0] != 1:
            raise RuntimeError("ClickHouse health query returned an unexpected result")
    finally:
        client.close()

    print("ClickHouse connection healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

