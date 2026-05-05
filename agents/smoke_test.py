import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_CONTAINERS = {
    "kafka",
    "schema-registry",
    "kafka-connect",
    "flink-jobmanager",
    "flink-taskmanager",
    "prometheus",
    "grafana",
}
EXPECTED_TOPICS = {
    "shoe-clickstream",
    "cart-updates",
    "inventory",
    "product-metadata",
    "live-user-profile",
    "live-product-profile",
    "recommendations",
}


def run(command):
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}{': ' + detail if detail else ''}")
    return ok


def docker_containers():
    result = run(["docker", "ps", "--format", "{{.Names}}"])
    if result.returncode != 0:
        return check("Docker containers", False, result.stdout.strip())

    running = set(result.stdout.splitlines())
    missing = sorted(EXPECTED_CONTAINERS - running)
    return check("Docker containers", not missing, f"missing={missing}" if missing else "all expected services running")


def flink_jobs():
    result = run(["docker", "exec", "flink-jobmanager", "/opt/flink/bin/flink", "list"])
    if result.returncode != 0:
        return check("Flink jobs", False, result.stdout.strip())

    required = ["live_user_profile", "live_product_profile"]
    missing = [job for job in required if job not in result.stdout]
    return check("Flink jobs", not missing, f"missing={missing}" if missing else "user and product profile jobs running")


def kafka_topics_and_offsets():
    result = run(["docker", "exec", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"])
    if result.returncode != 0:
        return check("Kafka topics", False, result.stdout.strip())

    topics = set(result.stdout.splitlines())
    missing = sorted(EXPECTED_TOPICS - topics)
    topics_ok = check("Kafka topics", not missing, f"missing={missing}" if missing else "all expected topics exist")

    offsets_ok = True
    for topic in sorted(EXPECTED_TOPICS - {"recommendations"}):
        offsets = run(["docker", "exec", "kafka", "kafka-get-offsets", "--bootstrap-server", "localhost:9092", "--topic", topic])
        if offsets.returncode != 0:
            offsets_ok = False
            check(f"Kafka offsets for {topic}", False, offsets.stdout.strip())
            continue

        total = 0
        for line in offsets.stdout.splitlines():
            parts = line.rsplit(":", 1)
            if len(parts) == 2 and parts[1].isdigit():
                total += int(parts[1])
        offsets_ok = check(f"Kafka offsets for {topic}", total > 0, f"messages={total}") and offsets_ok

    return topics_ok and offsets_ok


def read_recent_topic(topic, key_field, lookback_per_partition=20000):
    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )

    latest = {}
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return latest

        topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        for topic_partition in topic_partitions:
            end_offset = end_offsets.get(topic_partition, 0)
            consumer.seek(topic_partition, max(0, end_offset - lookback_per_partition))

        remaining = set(topic_partitions)
        deadline = time.time() + 3
        while remaining and time.time() < deadline:
            records = consumer.poll(timeout_ms=200, max_records=1000)
            if not records:
                break

            for topic_partition, messages in records.items():
                for message in messages:
                    value = message.value
                    if value and value.get(key_field) is not None:
                        latest[value[key_field]] = value

            for topic_partition in list(remaining):
                if consumer.position(topic_partition) >= end_offsets.get(topic_partition, 0):
                    remaining.remove(topic_partition)
    finally:
        consumer.close()

    return latest


def profiles_exist():
    users = read_recent_topic("live-user-profile", "userid")
    products = read_recent_topic("live-product-profile", "productid")
    users_ok = check("Live user profiles", len(users) > 0, f"latest_users={len(users)}")
    products_ok = check("Live product profiles", len(products) > 0, f"latest_products={len(products)}")

    if users:
        fields = ["userid", "active_interest_category", "price_sensitivity", "total_orders"]
        shaped_users = [
            profile for profile in users.values()
            if all(field in profile for field in fields)
        ]
        users_ok = check(
            "Live user profile shape",
            len(shaped_users) > 0,
            f"current_shape_users={len(shaped_users)}",
        ) and users_ok

    if products:
        fields = ["productid", "category", "stock_trend", "demand_score", "avg_rating"]
        shaped_products = [
            profile for profile in products.values()
            if all(field in profile for field in fields)
        ]
        products_ok = check(
            "Live product profile shape",
            len(shaped_products) > 0,
            f"current_shape_products={len(shaped_products)}",
        ) and products_ok

    return users_ok and products_ok


def ollama_model():
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return check("Ollama", False, str(exc))

    models = {model.get("name") for model in payload.get("models", [])}
    return check("Ollama qwen3.5:4b", "qwen3.5:4b" in models, f"models={sorted(models)}")


def main():
    checks = [
        docker_containers(),
        flink_jobs(),
        kafka_topics_and_offsets(),
        profiles_exist(),
        ollama_model(),
    ]

    if all(checks):
        print("\nSmoke test passed. The Kafka/Flink/Ollama learning stack is healthy.")
        return 0

    print("\nSmoke test failed. Check the failed rows above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
