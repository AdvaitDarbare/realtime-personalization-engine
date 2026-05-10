import json
import os
import random
import time
import uuid
from kafka import KafkaProducer


def load_env_file():
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as env_file:
        for line in env_file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)


PRODUCTS = [
    {"productid": "NK-001", "category": "running", "price_band": "medium"},
    {"productid": "NK-002", "category": "running", "price_band": "medium"},
    {"productid": "NK-003", "category": "running", "price_band": "medium"},
    {"productid": "NK-004", "category": "lifestyle", "price_band": "medium"},
    {"productid": "NK-005", "category": "lifestyle", "price_band": "low"},
    {"productid": "NK-006", "category": "racing", "price_band": "high"},
    {"productid": "NK-007", "category": "running", "price_band": "low"},
    {"productid": "NK-008", "category": "lifestyle", "price_band": "medium"},
    {"productid": "NK-009", "category": "running", "price_band": "high"},
    {"productid": "NK-010", "category": "training", "price_band": "high"},
    {"productid": "AD-001", "category": "running", "price_band": "high"},
    {"productid": "AD-002", "category": "lifestyle", "price_band": "medium"},
    {"productid": "AD-003", "category": "lifestyle", "price_band": "medium"},
    {"productid": "AD-004", "category": "lifestyle", "price_band": "high"},
    {"productid": "AD-005", "category": "lifestyle", "price_band": "low"},
    {"productid": "AD-006", "category": "football", "price_band": "high"},
    {"productid": "AD-007", "category": "lifestyle", "price_band": "medium"},
    {"productid": "AD-008", "category": "hiking", "price_band": "medium"},
    {"productid": "NB-001", "category": "running", "price_band": "high"},
    {"productid": "NB-002", "category": "lifestyle", "price_band": "low"},
    {"productid": "NB-003", "category": "running", "price_band": "high"},
    {"productid": "NB-004", "category": "lifestyle", "price_band": "low"},
    {"productid": "NB-005", "category": "training", "price_band": "medium"},
    {"productid": "AS-001", "category": "running", "price_band": "high"},
    {"productid": "AS-002", "category": "running", "price_band": "high"},
    {"productid": "AS-003", "category": "running", "price_band": "medium"},
    {"productid": "PU-001", "category": "lifestyle", "price_band": "low"},
    {"productid": "PU-002", "category": "lifestyle", "price_band": "low"},
    {"productid": "PU-003", "category": "running", "price_band": "medium"},
    {"productid": "VN-001", "category": "lifestyle", "price_band": "low"},
]

CATEGORIES = sorted({product["category"] for product in PRODUCTS})
USER_IDS = list(range(1, 101))

USER_PERSONAS = {
    userid: {
        "favorite_categories": random.sample(CATEGORIES, k=2),
        "price_band": random.choice(["low", "medium", "high"]),
    }
    for userid in USER_IDS
}

SEARCH_TERMS = {
    "running": ["daily running shoes", "cushioned trainers", "marathon shoes"],
    "lifestyle": ["white sneakers", "retro sneakers", "everyday shoes"],
    "racing": ["race day shoes", "carbon plate shoes", "fast running shoes"],
    "training": ["gym shoes", "cross training shoes", "stable trainers"],
    "football": ["football boots", "firm ground cleats", "control boots"],
    "hiking": ["trail shoes", "hiking shoes", "outdoor sneakers"],
}


def weighted_product_for_user(userid):
    persona = USER_PERSONAS[userid]
    preferred = [
        product for product in PRODUCTS
        if product["category"] in persona["favorite_categories"]
        or product["price_band"] == persona["price_band"]
    ]
    pool = preferred if random.random() < 0.8 else PRODUCTS
    return random.choice(pool)


def event_type_for_session_step(step):
    if step == 0:
        return random.choice(["search", "product_view"])
    return random.choices(
        ["product_view", "product_view", "add_to_cart", "search"],
        weights=[55, 20, 15, 10],
        k=1
    )[0]


def generate_event(userid, session_id, step):
    product = weighted_product_for_user(userid)
    event_type = event_type_for_session_step(step)
    category = product["category"]

    return {
        "event_id": str(uuid.uuid4()),
        "userid": userid,
        "session_id": session_id,
        "event_type": event_type,
        "productid": product["productid"] if event_type != "search" else None,
        "category": category,
        "query": random.choice(SEARCH_TERMS[category]) if event_type == "search" else None,
        "ts": int(time.time() * 1000),
    }


def send_clickstream_events():
    print("Starting clickstream producer...")
    print("Generating search, product_view, and add_to_cart events...")

    while True:
        userid = random.choice(USER_IDS)
        session_id = f"s-{userid}-{uuid.uuid4().hex[:8]}"
        session_length = random.randint(2, 8)

        for step in range(session_length):
            event = generate_event(userid, session_id, step)
            producer.send(
                topic="shoe-clickstream",
                key=userid,
                value=event
            )

            print(
                f"Clickstream: user {event['userid']} "
                f"{event['event_type']} {event['category']} "
                f"{event['productid'] or event['query']}"
            )
            time.sleep(random.uniform(0.2, 1.0))

        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    send_clickstream_events()
