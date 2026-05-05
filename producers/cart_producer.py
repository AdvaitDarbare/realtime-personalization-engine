import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Same product IDs as our inventory
PRODUCT_IDS = [
    "NK-001", "NK-002", "NK-003", "NK-004", "NK-005",
    "NK-006", "NK-007", "NK-008", "NK-009", "NK-010",
    "AD-001", "AD-002", "AD-003", "AD-004", "AD-005",
    "AD-006", "AD-007", "AD-008",
    "NB-001", "NB-002", "NB-003", "NB-004", "NB-005",
    "AS-001", "AS-002", "AS-003",
    "PU-001", "PU-002", "PU-003",
    "VN-001"
]

# Real prices matching our inventory
PRODUCT_PRICES = {
    "NK-001": 89.99, "NK-002": 119.99, "NK-003": 99.99,
    "NK-004": 90.00, "NK-005": 79.99,  "NK-006": 149.99,
    "NK-007": 79.99, "NK-008": 95.00,  "NK-009": 149.99,
    "NK-010": 130.00,"AD-001": 149.99, "AD-002": 85.00,
    "AD-003": 84.99, "AD-004": 129.99, "AD-005": 74.99,
    "AD-006": 139.99,"AD-007": 90.00,  "AD-008": 119.99,
    "NB-001": 184.99,"NB-002": 64.99,  "NB-003": 164.99,
    "NB-004": 74.99, "NB-005": 94.99,  "AS-001": 129.99,
    "AS-002": 159.99,"AS-003": 109.99, "PU-001": 65.00,
    "PU-002": 69.99, "PU-003": 109.99, "VN-001": 54.99
}

# User IDs matching Datagen clickstream (1-100)
USER_IDS = list(range(1, 101))

# Some users prefer cheaper shoes (price sensitive)
# Some users prefer expensive shoes (low price sensitivity)
USER_BUDGETS = {
    uid: random.choice(["low", "medium", "high"])
    for uid in USER_IDS
}

BUDGET_PRODUCTS = {
    "low": [p for p, price in PRODUCT_PRICES.items() if price < 80],
    "medium": [p for p, price in PRODUCT_PRICES.items() if 80 <= price <= 120],
    "high": [p for p, price in PRODUCT_PRICES.items() if price > 120]
}

order_id = 1000

def generate_order(userid):
    global order_id

    # Pick product based on user budget
    budget = USER_BUDGETS[userid]
    product_id = random.choice(BUDGET_PRODUCTS[budget])
    price = PRODUCT_PRICES[product_id]

    event = {
        "order_id": order_id,
        "userid": userid,
        "productid": product_id,
        "price": price,
        "action": random.choice(["purchase", "purchase", "purchase", "return"]),
        "ts": int(time.time() * 1000)
    }

    order_id += 1
    return event

def send_cart_events():
    print("Starting cart producer...")
    print("Generating realistic cart events with real product IDs...")

    while True:
        # Pick a random user
        userid = random.choice(USER_IDS)
        event = generate_order(userid)

        producer.send(
            topic='cart-updates',
            key=userid,
            value=event
        )

        print(f"Order: user {event['userid']} → {event['productid']} ${event['price']} ({event['action']})")
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    send_cart_events()