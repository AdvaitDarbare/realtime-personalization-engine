import json
import chromadb
from kafka import KafkaConsumer
from crewai.tools import tool

# Module-level cache — collection is built once per process on first tool call
_collection = None


def _build_collection() -> chromadb.Collection:
    """
    Read product metadata from Kafka, embed each product using ChromaDB's
    default local embedding model (all-MiniLM-L6-v2), and return the collection.
    The model is downloaded automatically on first run (~50MB, one time only).
    """
    consumer = KafkaConsumer(
        'product-metadata',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda m: m.decode('utf-8') if m else None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )

    # Keep only the latest message per product
    products: dict = {}
    for message in consumer:
        if message.value and message.value.get('productid'):
            products[message.value['productid']] = message.value
    consumer.close()

    client = chromadb.Client()  # in-memory, no disk persistence needed

    # Delete and recreate so reruns don't accumulate duplicate IDs
    try:
        client.delete_collection("products")
    except Exception:
        pass
    collection = client.create_collection("products")

    if not products:
        return collection

    ids, documents, metadatas = [], [], []
    for productid, p in products.items():
        attrs = ', '.join(p.get('attributes', []))
        doc = (
            f"{p.get('name', '')}: {p.get('description', '')}. "
            f"Attributes: {attrs}."
        )
        ids.append(productid)
        documents.append(doc)
        metadatas.append({
            "productid": productid,
            "name": p.get('name', ''),
            "avg_rating": float(p.get('avg_rating', 0.0)),
        })

    collection.add(documents=documents, ids=ids, metadatas=metadatas)
    return collection


def _get_collection() -> chromadb.Collection:
    global _collection
    if _collection is None:
        _collection = _build_collection()
    return _collection


@tool("Find Similar Products")
def find_similar_products(query: str) -> str:
    """Semantic search over the product catalog by meaning, not just category or price.
    Use this FIRST to discover which products are most relevant to this user's intent
    before looking up live stock and price.

    Good queries:
    - "lightweight cushioned daily running shoe"
    - "retro lifestyle sneaker streetwear"
    - "stable supportive shoe for overpronation"
    - "minimal natural movement training shoe"

    Returns the top 5 matching product IDs and names ranked by semantic similarity.
    Then use Get Live Product Profile or Get All Products to check live stock and price."""
    collection = _get_collection()
    if collection.count() == 0:
        return "Product index is empty — no product metadata found in Kafka."

    results = collection.query(query_texts=[query], n_results=min(5, collection.count()))

    matches = []
    for i, pid in enumerate(results['ids'][0]):
        meta = results['metadatas'][0][i]
        distance = results['distances'][0][i]
        similarity = round(1 - distance, 3)
        matches.append({
            "productid": pid,
            "name": meta.get('name', ''),
            "similarity_score": similarity,
        })

    return json.dumps(matches, indent=2)
