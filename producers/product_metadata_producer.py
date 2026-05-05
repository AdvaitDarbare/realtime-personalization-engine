import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

PRODUCT_METADATA = [
    {
        "productid": "NK-001",
        "name": "Nike Air Max 90",
        "description": "Iconic cushioning and style for everyday wear",
        "attributes": ["cushioned", "lightweight", "iconic", "street-style"],
        "avg_rating": 4.5,
        "review_count": 2341,
        "gender": "unisex",
        "weight_grams": 310,
        "drop_mm": 10,
        "similar_products": ["NK-002", "NK-005", "AD-003"]
    },
    {
        "productid": "NK-002",
        "name": "Nike Pegasus 40",
        "description": "Versatile daily trainer with responsive cushioning",
        "attributes": ["versatile", "responsive", "daily-trainer", "breathable"],
        "avg_rating": 4.6,
        "review_count": 1876,
        "gender": "unisex",
        "weight_grams": 283,
        "drop_mm": 10,
        "similar_products": ["NK-003", "NK-007", "AS-002"]
    },
    {
        "productid": "NK-003",
        "name": "Nike React Infinity",
        "description": "Maximum cushioning for long distance running",
        "attributes": ["max-cushion", "long-distance", "injury-prevention", "stable"],
        "avg_rating": 4.7,
        "review_count": 1543,
        "gender": "unisex",
        "weight_grams": 298,
        "drop_mm": 9,
        "similar_products": ["NK-002", "NB-003", "AS-001"]
    },
    {
        "productid": "NK-004",
        "name": "Nike Air Force 1",
        "description": "Classic basketball shoe turned streetwear icon",
        "attributes": ["classic", "streetwear", "iconic", "versatile"],
        "avg_rating": 4.8,
        "review_count": 5621,
        "gender": "unisex",
        "weight_grams": 420,
        "drop_mm": 0,
        "similar_products": ["NK-005", "NK-008", "AD-002"]
    },
    {
        "productid": "NK-005",
        "name": "Nike Dunk Low",
        "description": "Retro basketball style with modern comfort",
        "attributes": ["retro", "streetwear", "basketball", "colorful"],
        "avg_rating": 4.7,
        "review_count": 4312,
        "gender": "unisex",
        "weight_grams": 390,
        "drop_mm": 0,
        "similar_products": ["NK-004", "NK-008", "AD-003"]
    },
    {
        "productid": "NK-006",
        "name": "Nike Air Zoom Tempo",
        "description": "Carbon fiber plate for race day performance",
        "attributes": ["carbon-plate", "racing", "fast", "responsive"],
        "avg_rating": 4.6,
        "review_count": 876,
        "gender": "unisex",
        "weight_grams": 220,
        "drop_mm": 8,
        "similar_products": ["NK-003", "AS-001", "AD-001"]
    },
    {
        "productid": "NK-007",
        "name": "Nike Free Run 5.0",
        "description": "Natural motion feel for everyday runs",
        "attributes": ["natural", "flexible", "lightweight", "everyday"],
        "avg_rating": 4.3,
        "review_count": 987,
        "gender": "unisex",
        "weight_grams": 245,
        "drop_mm": 4,
        "similar_products": ["NK-002", "NB-005", "PU-003"]
    },
    {
        "productid": "NK-008",
        "name": "Nike Blazer Mid",
        "description": "High-top basketball heritage meets modern street style",
        "attributes": ["high-top", "heritage", "streetwear", "classic"],
        "avg_rating": 4.4,
        "review_count": 2134,
        "gender": "unisex",
        "weight_grams": 380,
        "drop_mm": 0,
        "similar_products": ["NK-004", "NK-005", "VN-001"]
    },
    {
        "productid": "NK-009",
        "name": "Nike Air Vapormax",
        "description": "Innovative air unit sole for maximum cushioning",
        "attributes": ["innovative", "max-air", "lightweight", "futuristic"],
        "avg_rating": 4.5,
        "review_count": 1654,
        "gender": "unisex",
        "weight_grams": 265,
        "drop_mm": 10,
        "similar_products": ["NK-001", "NK-003", "AD-001"]
    },
    {
        "productid": "NK-010",
        "name": "Nike Metcon 8",
        "description": "Stable and durable for cross-training and weightlifting",
        "attributes": ["stable", "durable", "cross-training", "gym"],
        "avg_rating": 4.6,
        "review_count": 1123,
        "gender": "unisex",
        "weight_grams": 340,
        "drop_mm": 4,
        "similar_products": ["NB-005", "PU-003", "AS-003"]
    },
    {
        "productid": "AD-001",
        "name": "Adidas Ultraboost 23",
        "description": "Premium energy return for long distance comfort",
        "attributes": ["boost", "energy-return", "premium", "long-distance"],
        "avg_rating": 4.7,
        "review_count": 3421,
        "gender": "unisex",
        "weight_grams": 310,
        "drop_mm": 10,
        "similar_products": ["NK-003", "NB-003", "AS-001"]
    },
    {
        "productid": "AD-002",
        "name": "Adidas Stan Smith",
        "description": "Timeless tennis shoe with clean minimal design",
        "attributes": ["minimal", "classic", "tennis", "clean"],
        "avg_rating": 4.6,
        "review_count": 6543,
        "gender": "unisex",
        "weight_grams": 290,
        "drop_mm": 0,
        "similar_products": ["AD-005", "AD-003", "VN-001"]
    },
    {
        "productid": "AD-003",
        "name": "Adidas Samba OG",
        "description": "Indoor football shoe turned cultural icon",
        "attributes": ["iconic", "retro", "football", "streetwear"],
        "avg_rating": 4.8,
        "review_count": 4231,
        "gender": "unisex",
        "weight_grams": 295,
        "drop_mm": 0,
        "similar_products": ["AD-002", "AD-005", "NK-004"]
    },
    {
        "productid": "AD-004",
        "name": "Adidas NMD R1",
        "description": "Street-ready boost cushioning with modern design",
        "attributes": ["boost", "streetwear", "modern", "comfortable"],
        "avg_rating": 4.4,
        "review_count": 2876,
        "gender": "unisex",
        "weight_grams": 285,
        "drop_mm": 10,
        "similar_products": ["AD-001", "AD-007", "NK-009"]
    },
    {
        "productid": "AD-005",
        "name": "Adidas Gazelle",
        "description": "Suede classic with a rich sporting heritage",
        "attributes": ["suede", "classic", "heritage", "retro"],
        "avg_rating": 4.5,
        "review_count": 3124,
        "gender": "unisex",
        "weight_grams": 270,
        "drop_mm": 0,
        "similar_products": ["AD-002", "AD-003", "PU-001"]
    },
    {
        "productid": "AD-006",
        "name": "Adidas Predator Edge",
        "description": "High performance football boot for precision control",
        "attributes": ["football", "precision", "control", "performance"],
        "avg_rating": 4.5,
        "review_count": 876,
        "gender": "unisex",
        "weight_grams": 210,
        "drop_mm": 0,
        "similar_products": ["AD-008", "NK-006"]
    },
    {
        "productid": "AD-007",
        "name": "Adidas Forum Low",
        "description": "Basketball heritage with bold retro styling",
        "attributes": ["retro", "basketball", "bold", "heritage"],
        "avg_rating": 4.3,
        "review_count": 1543,
        "gender": "unisex",
        "weight_grams": 380,
        "drop_mm": 0,
        "similar_products": ["AD-003", "NK-005", "NB-004"]
    },
    {
        "productid": "AD-008",
        "name": "Adidas Terrex Swift",
        "description": "Lightweight trail running for technical terrain",
        "attributes": ["trail", "lightweight", "outdoor", "technical"],
        "avg_rating": 4.6,
        "review_count": 765,
        "gender": "unisex",
        "weight_grams": 275,
        "drop_mm": 8,
        "similar_products": ["NB-003", "AS-002", "NK-007"]
    },
    {
        "productid": "NB-001",
        "name": "New Balance 990v5",
        "description": "Made in USA premium running with ultimate comfort",
        "attributes": ["premium", "made-in-usa", "comfortable", "durable"],
        "avg_rating": 4.8,
        "review_count": 2341,
        "gender": "unisex",
        "weight_grams": 340,
        "drop_mm": 12,
        "similar_products": ["NB-003", "AS-001", "AD-001"]
    },
    {
        "productid": "NB-002",
        "name": "New Balance 574",
        "description": "Classic everyday sneaker with encap cushioning",
        "attributes": ["classic", "everyday", "comfortable", "versatile"],
        "avg_rating": 4.5,
        "review_count": 3456,
        "gender": "unisex",
        "weight_grams": 310,
        "drop_mm": 0,
        "similar_products": ["NB-004", "AD-005", "PU-001"]
    },
    {
        "productid": "NB-003",
        "name": "New Balance Fresh Foam 1080",
        "description": "Ultra cushioned long distance daily trainer",
        "attributes": ["ultra-cushion", "long-distance", "soft", "premium"],
        "avg_rating": 4.7,
        "review_count": 1876,
        "gender": "unisex",
        "weight_grams": 298,
        "drop_mm": 8,
        "similar_products": ["NB-001", "AS-001", "NK-003"]
    },
    {
        "productid": "NB-004",
        "name": "New Balance 550",
        "description": "Retro basketball style with modern comfort updates",
        "attributes": ["retro", "basketball", "modern", "clean"],
        "avg_rating": 4.6,
        "review_count": 2187,
        "gender": "unisex",
        "weight_grams": 350,
        "drop_mm": 0,
        "similar_products": ["NB-002", "AD-007", "NK-005"]
    },
    {
        "productid": "NB-005",
        "name": "New Balance Minimus",
        "description": "Minimal drop training shoe for natural movement",
        "attributes": ["minimal", "natural", "training", "lightweight"],
        "avg_rating": 4.3,
        "review_count": 654,
        "gender": "unisex",
        "weight_grams": 198,
        "drop_mm": 4,
        "similar_products": ["NK-007", "NK-010", "PU-003"]
    },
    {
        "productid": "AS-001",
        "name": "ASICS Gel Kayano 30",
        "description": "Maximum stability and cushioning for overpronators",
        "attributes": ["stability", "max-cushion", "overpronation", "supportive"],
        "avg_rating": 4.7,
        "review_count": 2341,
        "gender": "unisex",
        "weight_grams": 310,
        "drop_mm": 10,
        "similar_products": ["AS-002", "NB-003", "NK-003"]
    },
    {
        "productid": "AS-002",
        "name": "ASICS Gel Nimbus 25",
        "description": "Plush cushioning for neutral runners going long",
        "attributes": ["plush", "neutral", "long-distance", "comfortable"],
        "avg_rating": 4.6,
        "review_count": 1654,
        "gender": "unisex",
        "weight_grams": 295,
        "drop_mm": 10,
        "similar_products": ["AS-001", "AS-003", "NB-003"]
    },
    {
        "productid": "AS-003",
        "name": "ASICS GT 2000 11",
        "description": "Supportive everyday trainer for mild overpronation",
        "attributes": ["supportive", "everyday", "mild-stability", "versatile"],
        "avg_rating": 4.5,
        "review_count": 1234,
        "gender": "unisex",
        "weight_grams": 275,
        "drop_mm": 10,
        "similar_products": ["AS-001", "AS-002", "NK-002"]
    },
    {
        "productid": "PU-001",
        "name": "Puma Suede Classic",
        "description": "Iconic suede sneaker with timeless street appeal",
        "attributes": ["suede", "iconic", "street", "classic"],
        "avg_rating": 4.4,
        "review_count": 2876,
        "gender": "unisex",
        "weight_grams": 290,
        "drop_mm": 0,
        "similar_products": ["AD-005", "AD-002", "VN-001"]
    },
    {
        "productid": "PU-002",
        "name": "Puma RS-X",
        "description": "Bold chunky retro running with RS cushioning",
        "attributes": ["chunky", "retro", "bold", "rs-cushion"],
        "avg_rating": 4.3,
        "review_count": 1543,
        "gender": "unisex",
        "weight_grams": 380,
        "drop_mm": 0,
        "similar_products": ["AD-004", "NK-009", "NB-004"]
    },
    {
        "productid": "PU-003",
        "name": "Puma Velocity Nitro 2",
        "description": "Nitro foam cushioning for energetic daily runs",
        "attributes": ["nitro-foam", "energetic", "daily-trainer", "responsive"],
        "avg_rating": 4.5,
        "review_count": 876,
        "gender": "unisex",
        "weight_grams": 265,
        "drop_mm": 8,
        "similar_products": ["NK-002", "AS-003", "NB-003"]
    },
    {
        "productid": "VN-001",
        "name": "Vans Old Skool",
        "description": "The original skate shoe with iconic side stripe",
        "attributes": ["skate", "iconic", "classic", "street"],
        "avg_rating": 4.6,
        "review_count": 7654,
        "gender": "unisex",
        "weight_grams": 350,
        "drop_mm": 0,
        "similar_products": ["NK-008", "PU-001", "AD-002"]
    },
]

def send_metadata():
    print("Starting product metadata producer...")
    print(f"Sending metadata for {len(PRODUCT_METADATA)} products...")

    # Send all product metadata once
    for product in PRODUCT_METADATA:
        event = {
            **product,
            "updated_at": int(time.time() * 1000)
        }
        producer.send(
            topic='product-metadata',
            key=product['productid'],
            value=event
        )
        print(f"Sent metadata: {product['name']} | rating: {product['avg_rating']} | reviews: {product['review_count']}")

    producer.flush()
    print("\nAll metadata sent!")

    # Periodically update ratings as new reviews come in
    while True:
        product = random.choice(PRODUCT_METADATA)
        event = {
            **product,
            "avg_rating": round(random.uniform(4.0, 5.0), 1),
            "review_count": product["review_count"] + random.randint(1, 5),
            "updated_at": int(time.time() * 1000)
        }
        producer.send(
            topic='product-metadata',
            key=product['productid'],
            value=event
        )
        print(f"Rating update: {product['name']} → {event['avg_rating']} ({event['review_count']} reviews)")
        time.sleep(10)

if __name__ == "__main__":
    send_metadata()