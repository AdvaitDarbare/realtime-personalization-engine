"""
Prometheus exporter for shoe-personalization business metrics.

Reads from three Kafka topics every SCRAPE_INTERVAL seconds:
  - live-user-profile    (written by Flink)
  - live-product-profile (written by Flink)
  - recommendations      (written by agents)

Exposes metrics on http://localhost:8888/metrics so Prometheus
(running in Docker) can scrape via host.docker.internal:8888.

Run:
    cd /path/to/shoe-personalization
    source agents/venv/bin/activate
    python monitoring/kafka_exporter.py
"""

import json
import time
import threading
import logging
from collections import defaultdict, Counter

from kafka import KafkaConsumer
from prometheus_client import start_http_server, Gauge, Counter as PromCounter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = "localhost:9092"
PORT = 8888
SCRAPE_INTERVAL = 15  # seconds between Kafka reads

# ─── Prometheus metrics ───────────────────────────────────────────────────────

users_by_sensitivity = Gauge(
    "shoe_users_total",
    "Number of users per price sensitivity tier",
    ["price_sensitivity"],
)
users_by_category = Gauge(
    "shoe_users_by_category",
    "Number of users per active interest category",
    ["category"],
)
avg_order_price = Gauge(
    "shoe_avg_order_price",
    "Mean avg_order_price across all live user profiles",
)
products_total = Gauge(
    "shoe_products_total",
    "Total number of products with a live profile",
)
products_low_stock = Gauge(
    "shoe_products_low_stock",
    "Number of products where stock_trend is low",
)
products_on_sale = Gauge(
    "shoe_products_on_sale",
    "Number of products currently on sale",
)
product_demand_score = Gauge(
    "shoe_product_demand_score",
    "Live demand score per product",
    ["productid", "name"],
)
product_stock = Gauge(
    "shoe_product_stock",
    "Current stock units per product",
    ["productid", "name"],
)
recommendations_total = Gauge(
    "shoe_recommendations_total",
    "Total recommendation events per agent type",
    ["agent_type"],
)


# ─── Kafka helpers ────────────────────────────────────────────────────────────

def read_latest(topic: str, key_field: str) -> dict:
    """Read all messages and return the latest value per key_field."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    records = {}
    for msg in consumer:
        if msg.value and msg.value.get(key_field) is not None:
            records[msg.value[key_field]] = msg.value
    consumer.close()
    return records


def read_all(topic: str) -> list:
    """Read all messages from a topic and return as a list (no dedup)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    records = [msg.value for msg in consumer if msg.value]
    consumer.close()
    return records


# ─── Metric update logic ──────────────────────────────────────────────────────

def update_user_metrics():
    profiles = read_latest("live-user-profile", "userid")
    if not profiles:
        log.warning("live-user-profile: no records found")
        return

    sensitivity_counts: Counter = Counter()
    category_counts: Counter = Counter()
    order_prices = []

    for p in profiles.values():
        sensitivity_counts[p.get("price_sensitivity", "unknown")] += 1
        cat = p.get("active_interest_category")
        if cat and cat != "unknown":
            category_counts[cat] += 1
        price = p.get("avg_order_price", 0)
        if price and price > 0:
            order_prices.append(price)

    for tier, count in sensitivity_counts.items():
        users_by_sensitivity.labels(price_sensitivity=tier).set(count)

    for cat, count in category_counts.items():
        users_by_category.labels(category=cat).set(count)

    if order_prices:
        avg_order_price.set(sum(order_prices) / len(order_prices))

    log.info(f"Users: {len(profiles)} profiles | sensitivity: {dict(sensitivity_counts)}")


def update_product_metrics():
    profiles = read_latest("live-product-profile", "productid")
    if not profiles:
        log.warning("live-product-profile: no records found")
        return

    low_stock = sum(1 for p in profiles.values() if p.get("stock_trend") == "low")
    on_sale = sum(1 for p in profiles.values() if p.get("on_sale"))

    products_total.set(len(profiles))
    products_low_stock.set(low_stock)
    products_on_sale.set(on_sale)

    for p in profiles.values():
        pid = p.get("productid", "")
        name = p.get("name", "")
        product_demand_score.labels(productid=pid, name=name).set(
            p.get("demand_score", 0)
        )
        product_stock.labels(productid=pid, name=name).set(p.get("stock", 0))

    log.info(f"Products: {len(profiles)} | low_stock: {low_stock} | on_sale: {on_sale}")


def update_recommendation_metrics():
    records = read_all("recommendations")
    if not records:
        log.warning("recommendations: no records found")
        return

    counts: Counter = Counter()
    for r in records:
        agent = r.get("agent_type", "unknown")
        counts[agent] += 1

    for agent, count in counts.items():
        recommendations_total.labels(agent_type=agent).set(count)

    log.info(f"Recommendations: {dict(counts)}")


def collect_all():
    log.info("--- collecting metrics ---")
    try:
        update_user_metrics()
    except Exception as e:
        log.error(f"user metrics error: {e}")
    try:
        update_product_metrics()
    except Exception as e:
        log.error(f"product metrics error: {e}")
    try:
        update_recommendation_metrics()
    except Exception as e:
        log.error(f"recommendation metrics error: {e}")


# ─── Main loop ────────────────────────────────────────────────────────────────

def run_loop():
    while True:
        collect_all()
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    log.info(f"Starting Prometheus exporter on :{PORT}")
    start_http_server(PORT)

    # Initial collection before entering the loop
    collect_all()

    # Run collection in a background thread so the HTTP server stays responsive
    t = threading.Thread(target=run_loop, daemon=True)
    t.start()

    log.info(f"Metrics available at http://localhost:{PORT}/metrics")
    log.info("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Exporter stopped.")
