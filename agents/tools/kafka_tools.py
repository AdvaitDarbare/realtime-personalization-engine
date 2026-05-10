# Agent thinks: "I need user data"
# Agent sees: "Get Live User Profile - Get the live profile for a specific user..."
# Agent calls: get_user_profile(42)
# Agent gets: {"userid": 42, "price_sensitivity": "medium", ...}


# Kafka topics (live data)
#       |
# read_latest_from_topic() - core reading logic
#       |
# get_user_profile()    - tool 1 wraps it
# get_product_profile() - tool 2 wraps it
# get_active_users()    - tool 3 wraps it
# get_all_products()    - tool 4 wraps it
#       |
# CrewAI agents call these tools
#       |
# write_recommendation() - writes output back to Kafka
#       |
# recommendations topic - downstream consumers can read this


import json
import os
import threading
import time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from crewai.tools import tool
from observability import trace_span

# PRODUCER (write recommendations back to Kafka)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_producer = None
_topic_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL_SECONDS = 10


def get_producer() -> KafkaProducer:
    """Create the Kafka producer lazily so imports work before Kafka is running."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
    return _producer

# HELPER FUNCTIONS

# we read recent messages from the topic and return the latest per key
def read_latest_from_topic(topic: str, key_field: str, lookback_per_partition: int = 20000) -> dict:
    """Read a bounded recent window from a topic and return latest per key."""
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda m: m.decode('utf-8') if m else None,
        enable_auto_commit=False,
        consumer_timeout_ms=2000
    )

    profiles = {}
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return profiles

        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        for topic_partition in topic_partitions:
            end_offset = end_offsets.get(topic_partition, 0)
            start_offset = max(0, end_offset - lookback_per_partition)
            consumer.seek(topic_partition, start_offset)

        remaining = set(topic_partitions)
        deadline = time.time() + 2
        while remaining and time.time() < deadline:
            records = consumer.poll(timeout_ms=200, max_records=1000)
            if not records:
                break

            for topic_partition, messages in records.items():
                for message in messages:
                    if message.value and message.value.get(key_field) is not None:
                        raw_key = message.value[key_field]
                        try:
                            key = int(raw_key)
                        except (ValueError, TypeError):
                            key = raw_key
                        profiles[key] = message.value

            for topic_partition in list(remaining):
                if consumer.position(topic_partition) >= end_offsets.get(topic_partition, 0):
                    remaining.remove(topic_partition)
    finally:
        consumer.close()

    return profiles


def cached_latest_from_topic(topic: str, key_field: str, lookback_per_partition: int = 20000) -> dict:
    """Cache short-lived topic snapshots so parallel tool calls reuse one Kafka scan."""
    now = time.time()
    cache_key = (topic, key_field, lookback_per_partition)
    with _cache_lock:
        cached = _topic_cache.get(cache_key)
        if cached and now - cached["created_at"] < CACHE_TTL_SECONDS:
            return cached["records"]

    records = read_latest_from_topic(topic, key_field, lookback_per_partition)

    with _cache_lock:
        _topic_cache[cache_key] = {
            "created_at": now,
            "records": records,
        }

    return records


# we write the recommendation back to the recommendations topic
def write_recommendation(userid: int, recommendation: str, agent_type: str, run_id: str | None = None):
    """Write agent output back to Kafka recommendations topic."""
    t0 = time.time()
    event = {
        "run_id": run_id,
        "userid": userid,
        "agent_type": agent_type,
        "recommendation": recommendation,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    producer = get_producer()
    producer.send(
        topic='recommendations',
        key=userid,
        value=event
    )
    producer.flush()
    trace_span(
        "kafka.write_recommendation",
        input={"topic": "recommendations", "userid": userid, "agent_type": agent_type},
        output=event,
        metadata={"topic": "recommendations"},
        start_time=t0,
    )


def latest_product_profiles() -> list[dict]:
    """Return the latest product profiles in a stable, compact shape."""
    profiles = cached_latest_from_topic(
        'live-product-profile',
        'productid',
        lookback_per_partition=250000,
    )
    products = []
    for product in profiles.values():
        price = product.get("price") or 0
        on_sale = bool(product.get("on_sale"))
        sale_price = product.get("sale_price")
        eff_price = float(sale_price) if on_sale and sale_price is not None else float(price)
        products.append({
            "productid": product.get("productid"),
            "name": product.get("name"),
            "brand": product.get("brand"),
            "category": product.get("category"),
            "price": price,
            "sale_price": sale_price,
            "on_sale": on_sale,
            "eff_price": round(eff_price, 2),
            "stock": product.get("stock"),
            "stock_trend": product.get("stock_trend"),
            "demand_score": product.get("demand_score"),
            "avg_rating": product.get("avg_rating"),
        })

    return products


def promotion_urgency(product: dict) -> tuple:
    """Rank products with deterministic live merchandising signals."""
    stock = product.get("stock") or 0
    demand_score = product.get("demand_score") or 0
    stock_trend = product.get("stock_trend")
    on_sale = bool(product.get("on_sale"))

    return (
        1 if stock_trend == "low" else 0,
        1 if stock <= 20 else 0,
        round(demand_score, 4),
        1 if on_sale else 0,
        product.get("avg_rating") or 0,
    )


def rank_promotion_candidates(limit: int = 3) -> list[dict]:
    t0 = time.time()
    products = latest_product_profiles()
    ranked = sorted(products, key=promotion_urgency, reverse=True)
    candidates = ranked[:limit]
    trace_span(
        "merchandising.rank_products",
        input={"candidate_count": len(products), "limit": limit},
        output={"candidates": candidates},
        metadata={"strategy": "low_stock_demand_sale_rating"},
        start_time=t0,
    )
    return candidates


def format_promotion_recommendation(candidates: list[dict]) -> str:
    lines = ["Top 3 products to promote right now:"]
    for idx, product in enumerate(candidates, start=1):
        channel = "push_notification" if product.get("stock_trend") == "low" else "homepage_banner"
        if product.get("on_sale"):
            channel = "email"

        sale_text = (
            f"sale price ${product['sale_price']:.2f}"
            if product.get("sale_price") is not None
            else "not on sale"
        )
        lines.extend([
            "",
            f"{idx}. {product['name']} ({product['productid']})",
            f"Reason: stock={product['stock']}, stock_trend={product['stock_trend']}, demand_score={product['demand_score']}, {sale_text}",
            f"Recommended channel: {channel}",
            "Expected impact: prioritize live demand while inventory and pricing signals are current",
        ])
    return "\n".join(lines)


def build_merchandising_recommendation() -> str:
    t0 = time.time()
    candidates = rank_promotion_candidates(limit=3)
    recommendation = format_promotion_recommendation(candidates)
    trace_span(
        "merchandising.final_recommendation",
        output=recommendation,
        metadata={"agent_type": "merchandising", "deterministic": True},
        start_time=t0,
    )
    return recommendation


# CREWAI TOOLS

@tool("Get Live User Profile")
def get_user_profile(userid: int) -> str:
    """Get the live profile for a specific user including
    recent intent-window behavior, orders, price sensitivity and preferences."""
    t0 = time.time()
    profile = None
    for lookback in (20000, 500000, 5_000_000):
        profiles = cached_latest_from_topic('live-user-profile', 'userid', lookback)
        profile = profiles.get(userid)
        if profile:
            break
    found = profile is not None
    trace_span(
        "kafka.get_user_profile",
        input={"topic": "live-user-profile", "userid": userid},
        output=profile if found else {"found": False},
        metadata={"topic": "live-user-profile"},
        start_time=t0,
    )
    return json.dumps(profile) if found else f"No profile found for user {userid}"


@tool("Get Live Product Profile")
def get_product_profile(productid: str) -> str:
    """Get the live profile for a specific product including
    stock levels, demand score, pricing and sale status."""
    t0 = time.time()
    # Try recent window first (fast); expand to full topic if not found.
    for lookback in (20000, 500000, 5_000_000):
        profiles = cached_latest_from_topic('live-product-profile', 'productid', lookback)
        profile = profiles.get(productid)
        if profile:
            break
    found = profile is not None
    trace_span(
        "kafka.get_product_profile",
        input={"topic": "live-product-profile", "productid": productid},
        output=profile if found else {"found": False},
        metadata={"topic": "live-product-profile"},
        start_time=t0,
    )
    return json.dumps(profile) if found else f"No profile found for product {productid}"


@tool("Get All Active Users")
def get_active_users() -> str:
    """Get latest profiles for all users who have placed orders."""
    t0 = time.time()
    profiles = cached_latest_from_topic('live-user-profile', 'userid')
    active = [p for p in profiles.values() if p.get('total_orders', 0) > 0]
    trace_span(
        "kafka.get_active_users",
        input={"topic": "live-user-profile"},
        output={"count": len(active), "sample": active[:10]},
        metadata={"topic": "live-user-profile"},
        start_time=t0,
    )
    return json.dumps(active[:10])


@tool("Get All Products")
def get_all_products() -> str:
    """Get latest profiles for all products including
    stock levels, demand scores, sale status, and eff_price.
    eff_price = sale_price when on_sale, else price.
    Use eff_price for all price comparisons."""
    t0 = time.time()
    products = latest_product_profiles()

    products.sort(
        key=lambda p: (
            str(p.get("category") or ""),
            not bool(p.get("on_sale")),
            -(p.get("demand_score") or 0),
        )
    )
    trace_span(
        "kafka.get_all_products",
        input={"topic": "live-product-profile"},
        output={"count": len(products), "sample": products[:10]},
        metadata={"topic": "live-product-profile"},
        start_time=t0,
    )
    return json.dumps(products)


@tool("Get Price Qualified Products")
def get_price_qualified_products(price_sensitivity: str, avg_order_price: float, category: str = "") -> str:
    """Return products that qualify for a given price sensitivity tier.

    Filters the catalog by eff_price (sale_price if on_sale, else price):
      - high   : eff_price <= 90
      - medium : 80 <= eff_price <= 120
      - low    : any eff_price (no filter)

    Args:
        price_sensitivity: "high", "medium", or "low"
        avg_order_price: user's average order price (for sorting)
        category: optional category filter (e.g. "running", "lifestyle")

    Returns qualifying products sorted by how close eff_price is to avg_order_price,
    then by demand_score descending. Use this instead of manually filtering Get All Products.
    """
    t0 = time.time()
    products = latest_product_profiles()
    price_sensitivity = str(price_sensitivity or "").lower()
    category = str(category or "")
    try:
        avg_order_price = float(avg_order_price)
    except (TypeError, ValueError):
        avg_order_price = 0.0

    qualified = []
    for p in products:
        eff = p.get("eff_price", p.get("price") or 0)
        if category and p.get("category") != category:
            continue
        if p.get("stock", 0) == 0:
            continue
        if price_sensitivity == "high" and eff <= 90:
            qualified.append(p)
        elif price_sensitivity == "medium" and 80 <= eff <= 120:
            qualified.append(p)
        elif price_sensitivity == "low":
            qualified.append(p)

    qualified.sort(key=lambda p: (
        abs((p.get("eff_price") or 0) - avg_order_price),
        -(p.get("demand_score") or 0),
    ))

    trace_span(
        "kafka.get_price_qualified_products",
        input={"price_sensitivity": price_sensitivity, "avg_order_price": avg_order_price, "category": category},
        output={"count": len(qualified), "products": [p["productid"] for p in qualified]},
        metadata={"topic": "live-product-profile"},
        start_time=t0,
    )
    return json.dumps(qualified)
