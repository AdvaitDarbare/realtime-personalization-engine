import json
import time
import random
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # this is where our kafka broker is running
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), # this is how we serialize the value to a JSON string
    key_serializer=lambda k: k.encode('utf-8') # this is how we serialize the key to a JSON string
)

# Our shoe inventory - 30 real products
PRODUCTS = [
    {"productid": "NK-001", "name": "Nike Air Max 90", "brand": "Nike", "category": "running", "price": 109.99, "sale_price": 89.99, "on_sale": True},
    {"productid": "NK-002", "name": "Nike Pegasus 40", "brand": "Nike", "category": "running", "price": 119.99, "sale_price": None, "on_sale": False},
    {"productid": "NK-003", "name": "Nike React Infinity", "brand": "Nike", "category": "running", "price": 129.99, "sale_price": 99.99, "on_sale": True},
    {"productid": "NK-004", "name": "Nike Air Force 1", "brand": "Nike", "category": "lifestyle", "price": 90.00, "sale_price": None, "on_sale": False},
    {"productid": "NK-005", "name": "Nike Dunk Low", "brand": "Nike", "category": "lifestyle", "price": 100.00, "sale_price": 79.99, "on_sale": True},
    {"productid": "NK-006", "name": "Nike Air Zoom Tempo", "brand": "Nike", "category": "racing", "price": 149.99, "sale_price": None, "on_sale": False},
    {"productid": "NK-007", "name": "Nike Free Run 5.0", "brand": "Nike", "category": "running", "price": 99.99, "sale_price": 79.99, "on_sale": True},
    {"productid": "NK-008", "name": "Nike Blazer Mid", "brand": "Nike", "category": "lifestyle", "price": 95.00, "sale_price": None, "on_sale": False},
    {"productid": "NK-009", "name": "Nike Air Vapormax", "brand": "Nike", "category": "running", "price": 189.99, "sale_price": 149.99, "on_sale": True},
    {"productid": "NK-010", "name": "Nike Metcon 8", "brand": "Nike", "category": "training", "price": 130.00, "sale_price": None, "on_sale": False},
    {"productid": "AD-001", "name": "Adidas Ultraboost 23", "brand": "Adidas", "category": "running", "price": 189.99, "sale_price": 149.99, "on_sale": True},
    {"productid": "AD-002", "name": "Adidas Stan Smith", "brand": "Adidas", "category": "lifestyle", "price": 85.00, "sale_price": None, "on_sale": False},
    {"productid": "AD-003", "name": "Adidas Samba OG", "brand": "Adidas", "category": "lifestyle", "price": 100.00, "sale_price": 84.99, "on_sale": True},
    {"productid": "AD-004", "name": "Adidas NMD R1", "brand": "Adidas", "category": "lifestyle", "price": 129.99, "sale_price": None, "on_sale": False},
    {"productid": "AD-005", "name": "Adidas Gazelle", "brand": "Adidas", "category": "lifestyle", "price": 90.00, "sale_price": 74.99, "on_sale": True},
    {"productid": "AD-006", "name": "Adidas Predator Edge", "brand": "Adidas", "category": "football", "price": 139.99, "sale_price": None, "on_sale": False},
    {"productid": "AD-007", "name": "Adidas Forum Low", "brand": "Adidas", "category": "lifestyle", "price": 90.00, "sale_price": None, "on_sale": False},
    {"productid": "AD-008", "name": "Adidas Terrex Swift", "brand": "Adidas", "category": "hiking", "price": 149.99, "sale_price": 119.99, "on_sale": True},
    {"productid": "NB-001", "name": "New Balance 990v5", "brand": "New Balance", "category": "running", "price": 184.99, "sale_price": None, "on_sale": False},
    {"productid": "NB-002", "name": "New Balance 574", "brand": "New Balance", "category": "lifestyle", "price": 79.99, "sale_price": 64.99, "on_sale": True},
    {"productid": "NB-003", "name": "New Balance Fresh Foam 1080", "brand": "New Balance", "category": "running", "price": 164.99, "sale_price": None, "on_sale": False},
    {"productid": "NB-004", "name": "New Balance 550", "brand": "New Balance", "category": "lifestyle", "price": 89.99, "sale_price": 74.99, "on_sale": True},
    {"productid": "NB-005", "name": "New Balance Minimus", "brand": "New Balance", "category": "training", "price": 94.99, "sale_price": None, "on_sale": False},
    {"productid": "AS-001", "name": "ASICS Gel Kayano 30", "brand": "ASICS", "category": "running", "price": 159.99, "sale_price": 129.99, "on_sale": True},
    {"productid": "AS-002", "name": "ASICS Gel Nimbus 25", "brand": "ASICS", "category": "running", "price": 159.99, "sale_price": None, "on_sale": False},
    {"productid": "AS-003", "name": "ASICS GT 2000 11", "brand": "ASICS", "category": "running", "price": 129.99, "sale_price": 109.99, "on_sale": True},
    {"productid": "PU-001", "name": "Puma Suede Classic", "brand": "Puma", "category": "lifestyle", "price": 65.00, "sale_price": None, "on_sale": False},
    {"productid": "PU-002", "name": "Puma RS-X", "brand": "Puma", "category": "lifestyle", "price": 89.99, "sale_price": 69.99, "on_sale": True},
    {"productid": "PU-003", "name": "Puma Velocity Nitro 2", "brand": "Puma", "category": "running", "price": 109.99, "sale_price": None, "on_sale": False},
    {"productid": "VN-001", "name": "Vans Old Skool", "brand": "Vans", "category": "lifestyle", "price": 65.00, "sale_price": 54.99, "on_sale": True},
]

def get_stock():
    return random.randint(5, 100)

def send_inventory():
    print("Starting inventory producer...")
    print(f"Sending {len(PRODUCTS)} products to Kafka...")

    # First send all products with initial stock
    for product in PRODUCTS:
        event = {
            **product,
            "stock": get_stock(),
            "sizes_available": random.sample([6, 7, 8, 9, 10, 11, 12], k=random.randint(3, 7)),
            "updated_at": int(time.time() * 1000)
        }

        producer.send(
            topic='inventory',
            key=product['productid'],
            value=event
        )
        print(f"Sent: {product['name']} | stock: {event['stock']} | on_sale: {product['on_sale']}")

    producer.flush()
    print("\nAll products sent! Now simulating stock changes...")

    # Continuously simulate stock changes
    while True:
        product = random.choice(PRODUCTS)
        event = {
            **product,
            "stock": get_stock(),
            "sizes_available": random.sample([6, 7, 8, 9, 10, 11, 12], k=random.randint(3, 7)),
            "updated_at": int(time.time() * 1000)
        }

        producer.send(
            topic='inventory',
            key=product['productid'],
            value=event
        )
        print(f"Stock update: {product['name']} → stock: {event['stock']}")
        time.sleep(5)

if __name__ == "__main__":
    send_inventory()