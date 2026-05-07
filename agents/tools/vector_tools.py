import json
import time
import chromadb
from kafka import KafkaConsumer
from crewai.tools import tool
from observability import trace_span

# Module-level cache — collection is built once per process on first tool call
_collection = None


def _read_inventory() -> dict:
    """Read latest inventory record per product to get category + price tier."""
    consumer = KafkaConsumer(
        'inventory',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    inventory = {}
    for msg in consumer:
        if msg.value and msg.value.get('productid'):
            inventory[msg.value['productid']] = msg.value
    consumer.close()
    return inventory


def _price_tier(price: float) -> str:
    if price < 80:
        return "budget"
    if price < 130:
        return "mid-range"
    return "premium"


def _build_collection() -> chromadb.Collection:
    """
    Build an in-memory ChromaDB collection with cosine distance.

    Documents are enriched with category, price tier, and sale status
    pulled from the inventory topic so queries on "budget running shoe"
    or "premium lifestyle sneaker" return meaningfully differentiated results.
    """
    t0 = time.time()

    # Read product metadata (descriptions, attributes)
    meta_consumer = KafkaConsumer(
        'product-metadata',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    products: dict = {}
    for message in meta_consumer:
        if message.value and message.value.get('productid'):
            products[message.value['productid']] = message.value
    meta_consumer.close()

    # Join with inventory for category + price
    inventory = _read_inventory()

    client = chromadb.Client()
    try:
        client.delete_collection("products")
    except Exception:
        pass

    # Fix 1: cosine distance — correct metric for sentence embeddings
    collection = client.create_collection(
        "products",
        metadata={"hnsw:space": "cosine"},
    )

    if not products:
        trace_span("vector.build_collection", output={"products_indexed": 0}, start_time=t0)
        return collection

    ids, documents, metadatas = [], [], []
    for productid, p in products.items():
        inv = inventory.get(productid, {})
        category   = inv.get('category', 'lifestyle')
        price      = float(inv.get('price') or 0)
        on_sale    = bool(inv.get('on_sale', False))
        sale_price = inv.get('sale_price')
        tier       = _price_tier(price)

        attrs = ', '.join(p.get('attributes', []))

        # Fix 2: enrich document with category, price tier, sale status
        # so queries like "budget running shoe" or "premium lifestyle sneaker"
        # return meaningfully differentiated results
        sale_text = (
            f"On sale at ${sale_price:.2f} (from ${price:.2f})."
            if on_sale and sale_price
            else f"Priced at ${price:.2f}."
        )
        doc = (
            f"{p.get('name', '')}: {p.get('description', '')}. "
            f"Category: {category}. "
            f"Price tier: {tier}. "
            f"{sale_text} "
            f"Attributes: {attrs}."
        )

        ids.append(productid)
        documents.append(doc)
        # Fix 3: store category + price_tier in metadata for where-filter support
        metadatas.append({
            "productid": productid,
            "name": p.get('name', ''),
            "category": category,
            "price_tier": tier,
            "avg_rating": float(p.get('avg_rating', 0.0)),
        })

    collection.add(documents=documents, ids=ids, metadatas=metadatas)
    trace_span(
        "vector.build_collection",
        output={"products_indexed": len(ids), "categories": list({m["category"] for m in metadatas})},
        metadata={"collection": "products", "distance": "cosine"},
        start_time=t0,
    )
    return collection


def _get_collection() -> chromadb.Collection:
    global _collection
    if _collection is None:
        _collection = _build_collection()
    return _collection


@tool("Find Similar Products")
def find_similar_products(query: str, category: str = "") -> str:
    """Semantic search over the product catalog.

    ALWAYS call this before Get All Products to discover the most relevant products.

    Args:
        query:    Rich 4-8 word description of what this user needs.
                  Include category + adjectives + price tier signal.
                  Examples:
                    "cushioned daily running shoe mid-range"
                    "premium carbon racing flat fast"
                    "budget retro lifestyle sneaker affordable"
                    "stable supportive trail running shoe"
                  Avoid bare "running shoe" or "lifestyle shoe" — too generic.

        category: (optional) Filter to a single category before ranking.
                  Pass the user's active_interest_category exactly as returned
                  by Get Live User Profile: "running", "lifestyle", "racing",
                  "training", "football", or "hiking".
                  Leave empty to search all categories.

    Returns top 5 product IDs ranked by cosine similarity (highest = best match).
    Then call Get All Products ONCE to retrieve live stock and pricing.
    """
    t0 = time.time()
    collection = _get_collection()
    if collection.count() == 0:
        return "Product index is empty — no product metadata found in Kafka."

    # Fix 3: apply category pre-filter when provided
    where = {"category": {"$eq": category}} if category else None
    n = min(5, collection.count())
    try:
        results = collection.query(query_texts=[query], n_results=n, where=where)
    except Exception:
        # Fall back to unfiltered if where-clause fails (e.g. no products in category)
        results = collection.query(query_texts=[query], n_results=n)

    matches = []
    for i, pid in enumerate(results['ids'][0]):
        meta     = results['metadatas'][0][i]
        distance = results['distances'][0][i]
        # cosine distance: 0 = identical, 2 = opposite; similarity = 1 - distance
        similarity = round(1 - distance, 3)
        matches.append({
            "productid":       pid,
            "name":            meta.get('name', ''),
            "category":        meta.get('category', ''),
            "price_tier":      meta.get('price_tier', ''),
            "similarity_score": similarity,
        })

    trace_span(
        "vector.find_similar_products",
        input={"query": query, "category_filter": category, "n_results": n},
        output=matches,
        metadata={"collection": "products", "distance": "cosine"},
        start_time=t0,
    )
    return json.dumps(matches, indent=2)
