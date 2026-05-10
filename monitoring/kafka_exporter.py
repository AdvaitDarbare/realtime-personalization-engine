import json
import os
import time
from collections import Counter

from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Gauge, start_http_server


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PORT = int(os.getenv("METRICS_PORT", "8888"))
SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "15"))


live_users_total = Gauge("shoe_live_users_total", "Live user profile count")
valid_user_profiles_total = Gauge("shoe_valid_user_profiles_total", "User profiles with known price sensitivity")
live_products_total = Gauge("shoe_live_products_total", "Live product profile count")
low_stock_products_total = Gauge("shoe_low_stock_products_total", "Products with low stock trend")
products_on_sale_total = Gauge("shoe_products_on_sale_total", "Products currently on sale")
avg_order_price_all = Gauge("shoe_avg_order_price_all", "Average order price across valid user profiles")
avg_effective_product_price = Gauge(
    "shoe_avg_effective_product_price",
    "Average effective product price after sale pricing",
)
recommendations_total = Gauge("shoe_recommendations_total", "Recommendation events by agent", ["agent_type"])
users_by_price_sensitivity = Gauge(
    "shoe_users_by_price_sensitivity",
    "Users by price sensitivity",
    ["price_sensitivity"],
)
users_by_active_category = Gauge(
    "shoe_users_by_active_category",
    "Users by active interest category",
    ["category"],
)
product_stock = Gauge("shoe_product_stock", "Latest stock by product", ["productid", "name"])
product_effective_price = Gauge(
    "shoe_product_effective_price",
    "Effective product price by product",
    ["productid", "name", "category"],
)
user_profile_info = Gauge(
    "shoe_user_profile_info",
    "Current user context as labels for Grafana table views",
    [
        "userid",
        "category",
        "price_sensitivity",
        "recent_searches",
        "recent_cart_adds",
        "total_orders",
        "avg_order_price",
    ],
)
product_profile_info = Gauge(
    "shoe_product_profile_info",
    "Current product context as labels for Grafana table views",
    [
        "productid",
        "name",
        "category",
        "price",
        "sale_price",
        "effective_price",
        "on_sale",
        "stock",
        "stock_trend",
        "demand_score",
    ],
)


def read_topic(topic: str, key_field: str | None = None, lookback_per_partition: int = 50000):
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    records = {} if key_field else []
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return records
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        for topic_partition in topic_partitions:
            consumer.seek(topic_partition, max(0, end_offsets[topic_partition] - lookback_per_partition))

        deadline = time.time() + 3
        while time.time() < deadline:
            batches = consumer.poll(timeout_ms=200, max_records=1000)
            if not batches:
                break
            for messages in batches.values():
                for message in messages:
                    if not message.value:
                        continue
                    if key_field:
                        key = message.value.get(key_field)
                        if key is not None:
                            records[key] = message.value
                    else:
                        records.append(message.value)
    finally:
        consumer.close()
    return records


def collect_once():
    users = read_topic("live-user-profile", "userid")
    products = read_topic("live-product-profile", "productid")
    recommendations = read_topic("recommendations")

    live_users_total.set(len(users))
    live_products_total.set(len(products))
    valid_users = [u for u in users.values() if u.get("price_sensitivity") != "unknown"]
    live_products = list(products.values())
    low_stock_products_total.set(sum(1 for p in live_products if p.get("stock_trend") == "low"))
    products_on_sale_total.set(sum(1 for p in live_products if p.get("on_sale")))
    valid_user_profiles_total.set(len(valid_users))

    order_prices = [float(u.get("avg_order_price") or 0) for u in valid_users if u.get("avg_order_price")]
    avg_order_price_all.set(sum(order_prices) / len(order_prices) if order_prices else 0)

    sensitivity_counts = Counter(p.get("price_sensitivity", "unknown") for p in users.values())
    category_counts = Counter(p.get("active_interest_category", "unknown") for p in users.values())
    for sensitivity, count in sensitivity_counts.items():
        users_by_price_sensitivity.labels(price_sensitivity=sensitivity).set(count)
    for category, count in category_counts.items():
        users_by_active_category.labels(category=category).set(count)

    recommendation_counts = Counter(r.get("agent_type", "unknown") for r in recommendations)
    for agent_type, count in recommendation_counts.items():
        recommendations_total.labels(agent_type=agent_type).set(count)

    effective_prices = []
    for product in live_products:
        price = float(product.get("price") or 0)
        sale_price = product.get("sale_price")
        on_sale = bool(product.get("on_sale"))
        effective_price = float(sale_price) if on_sale and sale_price is not None else price
        effective_prices.append(effective_price)
        product_stock.labels(
            productid=str(product.get("productid", "")),
            name=str(product.get("name", "")),
        ).set(product.get("stock") or 0)
        product_effective_price.labels(
            productid=str(product.get("productid", "")),
            name=str(product.get("name", "")),
            category=str(product.get("category", "")),
        ).set(effective_price)
        product_profile_info.labels(
            productid=str(product.get("productid", "")),
            name=str(product.get("name", "")),
            category=str(product.get("category", "")),
            price=f"{price:.2f}",
            sale_price="" if sale_price is None else f"{float(sale_price):.2f}",
            effective_price=f"{effective_price:.2f}",
            on_sale=str(on_sale).lower(),
            stock=str(product.get("stock") or 0),
            stock_trend=str(product.get("stock_trend", "")),
            demand_score=f"{float(product.get('demand_score') or 0):.2f}",
        ).set(1)

    avg_effective_product_price.set(sum(effective_prices) / len(effective_prices) if effective_prices else 0)

    for user in valid_users:
        user_profile_info.labels(
            userid=str(user.get("userid", "")),
            category=str(user.get("active_interest_category", "")),
            price_sensitivity=str(user.get("price_sensitivity", "")),
            recent_searches=str(user.get("recent_searches") or 0),
            recent_cart_adds=str(user.get("recent_cart_adds") or 0),
            total_orders=str(user.get("total_orders") or 0),
            avg_order_price=f"{float(user.get('avg_order_price') or 0):.2f}",
        ).set(1)

    print(
        f"metrics updated: users={len(users)} products={len(products)} recommendations={len(recommendations)}",
        flush=True,
    )


if __name__ == "__main__":
    start_http_server(PORT, addr="0.0.0.0")
    print(f"Kafka metrics exporter listening on http://localhost:{PORT}/metrics", flush=True)
    while True:
        collect_once()
        time.sleep(SCRAPE_INTERVAL_SECONDS)
