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
# recommendations topic - Grafana reads this


import json
import time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from crewai.tools import tool

# PRODUCER (write recommendations back to Kafka)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# HELPER FUNCTIONS

# we read recent messages from the topic and return the latest per key
def read_latest_from_topic(topic: str, key_field: str, lookback_per_partition: int = 20000) -> dict:
    """Read a bounded recent window from a topic and return latest per key."""
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
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


# we write the recommendation back to the recommendations topic
def write_recommendation(userid: int, recommendation: str, agent_type: str):
    """Write agent output back to Kafka recommendations topic."""
    event = {
        "userid": userid,
        "agent_type": agent_type,
        "recommendation": recommendation,
        "timestamp": __import__('time').strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.send(
        topic='recommendations',
        key=userid,
        value=event
    )
    producer.flush()


# CREWAI TOOLS

@tool("Get Live User Profile")
def get_user_profile(userid: int) -> str:
    """Get the live profile for a specific user including
    their page views, orders, price sensitivity and preferences."""
    profiles = read_latest_from_topic('live-user-profile', 'userid')
    profile = profiles.get(userid)
    if profile:
        return json.dumps(profile)
    return f"No profile found for user {userid}"


@tool("Get Live Product Profile")
def get_product_profile(productid: str) -> str:
    """Get the live profile for a specific product including
    stock levels, demand score, pricing and sale status."""
    profiles = read_latest_from_topic('live-product-profile', 'productid')
    profile = profiles.get(productid)
    if profile:
        return json.dumps(profile)
    return f"No profile found for product {productid}"


@tool("Get All Active Users")
def get_active_users() -> str:
    """Get latest profiles for all users who have placed orders."""
    profiles = read_latest_from_topic('live-user-profile', 'userid')
    active = [p for p in profiles.values() if p.get('total_orders', 0) > 0]
    return json.dumps(active[:10])


@tool("Get All Products")
def get_all_products() -> str:
    """Get latest profiles for all products including
    stock levels, demand scores and sale status."""
    profiles = read_latest_from_topic('live-product-profile', 'productid')
    products = []
    for product in profiles.values():
        products.append({
            "productid": product.get("productid"),
            "name": product.get("name"),
            "brand": product.get("brand"),
            "category": product.get("category"),
            "price": product.get("price"),
            "sale_price": product.get("sale_price"),
            "on_sale": product.get("on_sale"),
            "stock": product.get("stock"),
            "stock_trend": product.get("stock_trend"),
            "demand_score": product.get("demand_score"),
            "avg_rating": product.get("avg_rating"),
        })

    products.sort(
        key=lambda p: (
            str(p.get("category") or ""),
            not bool(p.get("on_sale")),
            -(p.get("demand_score") or 0),
        )
    )
    return json.dumps(products)
