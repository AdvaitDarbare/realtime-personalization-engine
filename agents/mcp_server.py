import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

os.environ.setdefault("DISABLE_LOCAL_TRACE", "true")
logging.getLogger("kafka").setLevel(logging.WARNING)

from tools.kafka_tools import (
    get_all_products,
    get_price_qualified_products,
    get_product_profile,
    get_user_profile,
    write_recommendation,
)
from tools.vector_tools import find_similar_products


ROOT = Path(__file__).resolve().parents[1]


def load_env_file() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_json(payload: str) -> Any:
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload


load_env_file()

mcp = FastMCP(
    "shoe-personalization-context",
    instructions=(
        "Small MCP bridge for the real-time shoe personalization demo. "
        "Tools expose live Kafka/Flink context and product search without "
        "requiring clients to know the Kafka topic layout."
    ),
    log_level="WARNING",
)


@mcp.tool()
def get_live_user_profile(userid: int) -> Any:
    """Return the latest live user profile built by Flink."""
    return parse_json(get_user_profile.func(userid))


@mcp.tool()
def get_live_product_profile(productid: str) -> Any:
    """Return the latest live product profile built by Flink."""
    return parse_json(get_product_profile.func(productid))


@mcp.tool()
def get_live_product_catalog() -> Any:
    """Return all latest product profiles with pricing, stock, and demand signals."""
    return parse_json(get_all_products.func())


@mcp.tool()
def get_price_qualified_catalog(
    price_sensitivity: str,
    avg_order_price: float,
    category: str = "",
) -> Any:
    """Return products that match a user's price sensitivity and optional category."""
    return parse_json(
        get_price_qualified_products.func(
            price_sensitivity=price_sensitivity,
            avg_order_price=avg_order_price,
            category=category,
        )
    )


@mcp.tool()
def search_similar_products(query: str, category: str = "") -> Any:
    """Search the product metadata index with a natural-language product need."""
    return parse_json(find_similar_products.func(query=query, category=category))


@mcp.tool()
def publish_recommendation(userid: int, recommendation: str, agent_type: str = "mcp-client") -> dict:
    """Write a recommendation event back to Kafka."""
    write_recommendation(
        userid=userid,
        recommendation=recommendation,
        agent_type=agent_type,
    )
    return {
        "ok": True,
        "topic": "recommendations",
        "userid": userid,
        "agent_type": agent_type,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the optional MCP context server.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="Transport for MCP clients. Use stdio for local desktop clients.",
    )
    parser.add_argument("--host", default=os.getenv("MCP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MCP_PORT", "8000")))
    args = parser.parse_args()

    if args.transport == "streamable-http":
        mcp.settings.host = args.host
        mcp.settings.port = args.port

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
