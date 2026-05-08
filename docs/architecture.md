# Architecture Deep Dive

This document covers every component, its responsibilities, its data contracts, and how they connect. Read it alongside the code — the files show how things work, this explains why the pieces exist and how they fit together.

## System Responsibilities

Each layer owns a narrow slice of the problem and communicates only through Kafka topics. Nothing calls Flink directly. Flink doesn't know agents exist. The agents don't know who published the profiles they read.

| Layer | Owned by | Responsibility |
|---|---|---|
| Event creation | Python producers | Simulate retail events (click, cart, inventory, metadata) |
| Event transport | Kafka | Durable ordered log; contract between all systems |
| Feature computation | Flink SQL | Turn raw event streams into live user and product profiles |
| Semantic retrieval | ChromaDB | Embed product descriptions; find similar products by meaning |
| Decision + explanation | CrewAI + gpt-4o-mini | Read profiles, run vector search, recommend product |
| Live business metrics | Prometheus + Grafana | Convert Kafka state to time-series dashboards |
| Agent observability | Langfuse | Record every tool call, input, output, and latency |
| Kafka visibility | Redpanda Console | Inspect topics, offsets, consumer groups |

## Full System Map

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Python Processes (running on your Mac)                                     │
│                                                                             │
│  clickstream_producer.py  cart_producer.py  inventory_producer.py           │
│  product_metadata_producer.py               monitoring/kafka_exporter.py    │
│  agents/main.py  agents/crew.py  agents/tools/                              │
└────────────┬──────────────┬──────────────┬──────────────┬────────────────┘
             │              │              │              │
             ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Kafka (Docker)                                                              │
│                                                                             │
│  shoe-clickstream    cart-updates    inventory    product-metadata           │
│                  ↓              ↓              ↓                            │
│             live-user-profile       live-product-profile                    │
│                  ↑              ↑              ↑                            │
│            [Flink upsert]   [Flink upsert]                                  │
│                                                                             │
│  recommendations  ←  agents write here                                      │
└──────┬───────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Docker Services                                                             │
│                                                                             │
│  flink-jobmanager   flink-taskmanager   prometheus   grafana                │
│  langfuse           langfuse-db         redpanda-console                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Kafka: Topics and Data Contracts

Kafka is the single integration point. Every producer and consumer knows only about topics — not about each other. This decoupling means you can add a new consumer (e.g., a fraud scorer) without touching any existing code.

### Raw Event Topics

These topics append events forever. They are the source of truth for what happened.

#### `shoe-clickstream`
- **Producer:** `clickstream_producer.py`
- **Consumer:** Flink `live_user_profile` job
- **Key:** `userid` (string)
- **Retention:** unlimited append log
- **Schema:**
```json
{
  "event_id": "uuid",
  "userid": 42,
  "session_id": "uuid",
  "event_type": "product_view | search | add_to_cart",
  "productid": "NK-007",
  "category": "running",
  "query": "cushioned running shoe",
  "ts": 1760000000000
}
```
- `ts` is milliseconds since epoch and is used as Flink event time via `TO_TIMESTAMP_LTZ(ts, 3)`
- `query` is only populated for `search` events
- `productid` and `category` are only populated for `product_view` and `add_to_cart` events

#### `cart-updates`
- **Producer:** `cart_producer.py`
- **Consumer:** Flink `live_user_profile` job (for order history) and `live_product_profile` job (for demand)
- **Key:** `userid` (string)
- **Schema:**
```json
{
  "order_id": 1001,
  "userid": 42,
  "productid": "NK-007",
  "price": 79.99,
  "action": "purchase | return",
  "ts": 1760000000000
}
```
- `price` is the transaction price, not the current catalog price. This makes average order price historically accurate even as catalog prices change.

#### `inventory`
- **Producer:** `inventory_producer.py`
- **Consumer:** Flink `live_product_profile` job; ChromaDB builder in vector_tools.py
- **Key:** `productid` (string)
- **Schema:**
```json
{
  "productid": "NK-007",
  "name": "Nike Free Run 5.0",
  "brand": "Nike",
  "category": "running",
  "price": 99.99,
  "sale_price": 79.99,
  "on_sale": true,
  "stock": 53,
  "updated_at": 1760000000000
}
```
- Updated continuously as stock changes, sale toggles, or prices update
- The vector tool reads this topic to enrich embeddings with price tier and sale status

#### `product-metadata`
- **Producer:** `product_metadata_producer.py`
- **Consumer:** Flink `live_product_profile` job; ChromaDB builder (for descriptions and attributes)
- **Key:** `productid` (string)
- **Schema:**
```json
{
  "productid": "NK-007",
  "name": "Nike Free Run 5.0",
  "description": "Lightweight and flexible daily trainer...",
  "attributes": ["cushioned", "breathable", "daily trainer"],
  "avg_rating": 4.3,
  "review_count": 284,
  "updated_at": 1760000000000
}
```
- `description` and `attributes` are used by ChromaDB to build the semantic index
- `avg_rating` ends up in the live product profile and is returned to agents

### Profile Topics (Upsert / Log-Compacted)

These topics behave differently from raw event topics. They maintain only the latest state per key. Kafka's `log compaction` policy ensures older versions of the same key are eventually removed, keeping the topic bounded.

#### `live-user-profile`
- **Producer:** Flink `live_user_profile` job via `upsert-kafka` connector
- **Consumer:** Agent loop (`main.py`), Prometheus exporter
- **Key:** `userid` (integer as JSON)
- **Compaction:** `cleanup.policy=compact`
- **Schema:**
```json
{
  "userid": 42,
  "recent_page_views": 7,
  "recent_searches": 2,
  "recent_cart_adds": 1,
  "active_interest_category": "running",
  "active_interest_events": 4,
  "intent_window_minutes": 15,
  "total_orders": 24,
  "total_purchases": 19,
  "total_returns": 5,
  "avg_order_price": 97.5,
  "price_sensitivity": "medium",
  "updated_at": "2026-05-08 10:30:00.000"
}
```
- `recent_*` fields come from the 15-minute HOP window and reset with each window
- `total_orders`, `avg_order_price`, and `price_sensitivity` are cumulative from cart history
- `price_sensitivity` is `unknown` until at least one cart event arrives (Flink cold start)

#### `live-product-profile`
- **Producer:** Flink `live_product_profile` job
- **Consumer:** Agents (via `get_price_qualified_products`, `get_all_products`), Prometheus exporter
- **Key:** `productid` (string as JSON)
- **Compaction:** `cleanup.policy=compact`
- **Schema:**
```json
{
  "productid": "NK-007",
  "name": "Nike Free Run 5.0",
  "brand": "Nike",
  "category": "running",
  "price": 99.99,
  "sale_price": 79.99,
  "on_sale": true,
  "stock": 53,
  "total_orders": 162,
  "demand_score": 1.62,
  "stock_trend": "high",
  "avg_rating": 4.3,
  "updated_at": "2026-05-08 10:30:00.000"
}
```
- `demand_score = total_orders / 100.0` — normalized demand signal
- `stock_trend`: `low` (<20), `medium` (20–49), `high` (≥50)
- The Python layer adds `eff_price = sale_price if on_sale else price` before returning to agents

#### `recommendations`
- **Producer:** Agents via `write_recommendation()`
- **Consumer:** Prometheus exporter, Langfuse (via trace output)
- **Key:** `userid` (integer)
- **Schema:**
```json
{
  "run_id": "personalization-a3f8b2c1d4e5",
  "userid": 42,
  "agent_type": "personalization | merchandising",
  "recommendation": "Product: Nike Free Run 5.0 (NK-007)...",
  "timestamp": "2026-05-08 10:30:05"
}
```

## Flink SQL: Streaming Jobs

Flink SQL turns raw Kafka topics into continuously updated profile tables. Two streaming jobs run permanently.

### Source Tables

Flink `CREATE TABLE` maps a Kafka topic to a typed schema. When `INSERT INTO` begins, Flink starts reading from that topic and never stops.

All source tables use `scan.startup.mode = 'latest-offset'`. This means Flink only processes events that arrive after the job starts. Historical events in the topic are not replayed. This is a trade-off: startup is fast, but profile counts restart from zero after a Flink restart.

**clickstream** uses event time with a 5-second watermark:
```sql
event_time AS TO_TIMESTAMP_LTZ(ts, 3),
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```
This tells Flink to use the producer timestamp for window assignment, and to wait up to 5 seconds for late-arriving events before closing a window.

**cart_updates**, **inventory**, and **product_metadata** use `PROCTIME()` — they have no event-time watermark and their windows use processing time instead.

### Views: Reusable Streaming Queries

Views are defined once and referenced in the `INSERT INTO` statement. Each view is a streaming query that continuously processes new rows.

**user_pageviews** — counts product views in 15-minute hopping windows:
```sql
SELECT userid, window_end, COUNT(*) AS recent_page_views
FROM TABLE(HOP(TABLE clickstream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '15' MINUTE))
WHERE event_type = 'product_view'
GROUP BY userid, window_start, window_end
```

A HOP window with slide=1m and size=15m creates one row per user per minute, where each row covers the previous 15 minutes. The result is a "rolling 15-minute window" that advances every minute.

**user_intent_totals** — counts searches and cart-adds in the same windows.

**user_category_interest** + **ranked_user_category_interest** — counts events per category per window, then uses `ROW_NUMBER()` to pick the most-active category per user per window. This is the `active_interest_category` field.

**user_orders** — plain `GROUP BY` aggregation over all cart_updates since startup. Not windowed — tracks cumulative order history.

**product_orders** — counts orders per product. Used for demand scoring.

### Sink Tables

`live_user_profile` and `live_product_profile` use the `upsert-kafka` connector. This connector requires a `PRIMARY KEY` declaration. Every time Flink writes a row for `userid=42`, Kafka receives a message with key=42 that replaces the previous value for that key in consumer eyes.

The upsert connector also emits delete tombstones — messages with a null value — when a row is retracted. This maintains correct changelog semantics for consumers that interpret the topic as a changing table.

### The User Profile INSERT Job

The `INSERT INTO live_user_profile` query joins four views:

```
user_intent_totals  ← HOP window anchor (sliding window rows)
    LEFT JOIN user_orders ON userid        ← cumulative cart history
    LEFT JOIN user_pageviews ON userid + window_end   ← same-window views
    LEFT JOIN ranked_user_category_interest ON userid + window_end + rank=1
```

The HOP window is the driver: for each (userid, window_end) pair in `user_intent_totals`, Flink emits one profile row. The LEFT JOINs attach cumulative order history and same-window page views and category data.

`COALESCE` handles nulls from LEFT JOINs — a user with no orders yet gets `total_orders=0` and `price_sensitivity='unknown'`.

### The Product Profile INSERT Job

The `INSERT INTO live_product_profile` query joins three streams:

```
inventory       ← current stock, price, sale status
    LEFT JOIN product_orders ON productid   ← demand count since startup
    LEFT JOIN product_metadata ON productid  ← ratings
```

Since inventory is a continuous stream, this job emits a new product profile row every time an inventory update arrives. The demand count accumulates from the first cart event after Flink starts.

## ChromaDB: Semantic Product Index

ChromaDB is an in-memory vector database. It is built once per agent process on the first call to `find_similar_products`.

### Build Process

`_build_collection()` in `vector_tools.py`:

1. Reads all messages from `product-metadata` topic (full scan from earliest)
2. Reads all messages from `inventory` topic (full scan) to get category, price, and sale status
3. Creates a ChromaDB collection with `hnsw:space = cosine` (cosine distance, not L2)
4. For each product, builds an enriched document string:
```
"Nike Free Run 5.0: Lightweight flexible daily trainer for everyday running. Category: running. Price tier: mid-range. On sale at $79.99 (from $99.99). Attributes: cushioned, breathable, daily trainer."
```
5. Stores metadata alongside each document: `{productid, name, category, price_tier, avg_rating}`
6. Adds all documents to ChromaDB, which embeds them using `all-MiniLM-L6-v2`

The `all-MiniLM-L6-v2` model is downloaded once (~79MB) and cached at `~/.cache/chroma/`. It runs entirely on-device — no API call required.

### Why Cosine Distance

`all-MiniLM-L6-v2` produces normalized unit-vector embeddings. For unit vectors, cosine distance and inner product are equivalent, and both are meaningful measures of semantic similarity. L2 distance (Euclidean) is **not** meaningful for normalized vectors because two vectors that point in the same direction (same meaning) but have different magnitudes would appear far apart. The ChromaDB default is L2, which gives incorrect rankings for sentence embeddings. This project explicitly overrides to cosine.

Cosine similarity formula: `similarity = 1 - cosine_distance`. Scores range from -1 (opposite meaning) to 1 (identical meaning). Typical in-catalog scores are 0.45–0.65.

### Why Documents Are Enriched

A thin document `"Nike Free Run 5.0: Lightweight daily trainer. Attributes: cushioned, breathable."` will score similarly for any running-related query. Enriching with category, price tier, and sale status makes the embedding carry more signal:

- Query `"budget everyday running shoe active shopper"` → matches enriched document with "budget" tier and "running" category more distinctively
- Query `"premium cushioned marathon trainer"` → pulls premium running shoes higher
- Category pre-filter narrows candidates before ranking, preventing cross-category matches

### Query and Retrieval

```python
where = {"category": {"$eq": category}} if category else None
results = collection.query(query_texts=[query], n_results=5, where=where)
```

ChromaDB embeds the query string with the same model, computes cosine distance to all stored documents (filtered by the WHERE clause), and returns the 5 nearest with their distances. `similarity_score = 1 - distance`.

## CrewAI Agents

CrewAI is the agent orchestration framework. It provides `Agent`, `Task`, `Crew`, and `Process` abstractions.

### Agent

An agent has:
- **role**: a noun phrase describing its function ("Personalization Specialist")
- **goal**: a sentence describing what it optimizes for
- **backstory**: context that shapes its persona and reasoning style
- **tools**: a list of callable Python functions it can invoke
- **llm**: which language model to use

### Task

A task has:
- **description**: the full natural-language instruction — what to do, what tools to call, in what order, how to select a result
- **expected_output**: a specification of what the final answer must look like
- **agent**: which agent executes this task

### Crew

A crew assembles agents and tasks and kicks off execution. `Process.sequential` means tasks run one after another in order. The output of the crew is the string output of the last task.

### LLM Tool Calling Loop

When a crew runs, the LLM receives the task description plus its available tool schemas. It decides which tools to call, in what order, with what arguments. CrewAI intercepts these calls, executes the actual Python function, and returns the result to the LLM. The LLM continues reasoning until it produces a final answer.

This loop is invisible in `crew.py` — it happens inside `crew.kickoff()`. What is visible is the tool call trace in Langfuse and the `verbose=True` output in the terminal.

## Agent Design: Personalization

**Tools available:** `find_similar_products`, `get_user_profile`, `get_price_qualified_products`

`get_all_products` is intentionally **not** given to the personalization agent. If it were, the agent could — and did, historically — skip the price-qualified tool and do its own filtering, getting it wrong. Removing the escape hatch forces the correct path.

**Task flow:**
1. `get_user_profile(userid)` → category, price_sensitivity, avg_order_price, behavior signals
2. `find_similar_products(query, category)` → top-5 by cosine similarity
3. `get_price_qualified_products(price_sensitivity, avg_order_price, category)` → filtered by price tier in Python
4. LLM cross-references both lists, picks highest-similarity product that appears in qualified set
5. Demand-score tiebreaker if top candidates within 0.05 similarity

**Why this design:** The LLM is good at building varied natural-language queries and explaining choices. It is unreliable at arithmetic comparisons (e.g., "is $79.99 < $80?"). Enforcing price tiers in code removes the arithmetic burden from the LLM entirely.

## Agent Design: Merchandising

**Tools available:** `get_all_products`, `get_active_users`

The merchandising agent is **fully deterministic** — it does not invoke the LLM for ranking. `build_merchandising_recommendation()` in `kafka_tools.py` ranks all products by a tuple:

```python
(1 if stock_trend == "low" else 0,
 1 if stock <= 20 else 0,
 round(demand_score, 4),
 1 if on_sale else 0,
 avg_rating)
```

Tuples compare element-by-element, so a product must have low stock trend to rank above a medium-stock product, then total stock ≤ 20 breaks ties, then demand_score, then on_sale, then avg_rating.

The channel recommendation is also rule-based: low stock → push_notification, on sale → email, otherwise → homepage_banner.

The LLM is only used as a CrewAI formality — the task description asks it to display the pre-ranked result. In practice, `run_merchandising()` calls `build_merchandising_recommendation()` directly and writes the result to Kafka, bypassing the LLM entirely.

## Observability

### Langfuse: Agent Traces

`observability.py` provides `trace_context()` and `trace_span()`:

```python
with trace_context("personalization-agent-run", run_id=run_id, user_id=userid):
    # everything inside this block is attached to one Langfuse trace
    result = crew.kickoff()
```

Every tool call records a span: name, input, output, latency. The `start_time` parameter uses a `time.time()` snapshot taken before the tool executes, so latency reflects actual execution time rather than SDK overhead.

Langfuse traces are only written when `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` are set. Without them, tracing is silently skipped.

### Prometheus + Grafana: Business Metrics

`monitoring/kafka_exporter.py` reads the profile topics and exposes metrics at `http://localhost:8888/metrics`. Prometheus scrapes this endpoint. Grafana queries Prometheus.

Key metrics exposed:
- `shoe_users_total{price_sensitivity}` — user count by sensitivity tier
- `shoe_users_by_category{category}` — user count by active interest category
- `shoe_products_total`, `shoe_products_low_stock`, `shoe_products_on_sale`
- `shoe_product_demand_score{productid, name}` — per-product demand
- `shoe_product_stock{productid, name}` — per-product inventory
- `shoe_recommendations_total{agent_type}` — recommendation count by agent

### Redpanda Console

Provides a web UI for Kafka topic inspection. Every message is browsable with its key, value, partition, offset, and timestamp. Consumer group lag is visible. No CLI needed for day-to-day debugging.

## Deployment Topology

Docker services use an internal network `kafka-network`. External Python processes connect via `localhost:*` port mappings. Inside Docker, services use container hostnames.

| Service | External port | Internal address | Role |
|---|---|---|---|
| Kafka broker | `localhost:9092` | `kafka:29092` | Kafka broker |
| Schema Registry | `localhost:8081` | `schema-registry:8081` | Avro schema store |
| Kafka Connect | `localhost:8083` | — | Connectors (not used in this project) |
| Flink JobManager | `localhost:8080` | `flink-jobmanager:8081` | Job submission UI |
| Flink TaskManager | — | `flink-taskmanager` | Job execution (2048m memory) |
| Prometheus | `localhost:9090` | `prometheus:9090` | Metrics store |
| Grafana | `localhost:3000` | `grafana:3000` | Dashboards (admin/admin) |
| Langfuse | `localhost:3001` | `langfuse:3000` | Agent traces |
| Langfuse DB | — | `langfuse-db:5432` | Postgres for Langfuse |
| Redpanda Console | `localhost:8088` | — | Kafka UI |

The Kafka container advertises two listener addresses:
- `PLAINTEXT://kafka:29092` — for containers on the `kafka-network`
- `PLAINTEXT_HOST://localhost:9092` — for processes on the host Mac

Flink is configured with 2048m process memory (`taskmanager.memory.process.size: 2048m`) and `taskmanager.memory.managed.fraction: 0.1` to reduce JVM overhead and prevent OOM on local machines. `restart: on-failure` handles transient startup failures.

## Data Catalog: 30 Products

The catalog contains 30 shoes across 6 categories. Effective prices and sensitivity tiers determine which products qualify for each user.

| Category | Count | Price range | Notes |
|---|---|---|---|
| lifestyle | 13 | $65–$130 | Most users, broadest range |
| running | 12 | $100–$190 | Many premium options, only NK-007 budget |
| training | 2 | $95–$130 | Small selection |
| football | 1 | $140 | AD-006 only |
| hiking | 1 | $150 | AD-008 only |
| racing | 1 | $150 | NK-006 only |

Price sensitivity tiers applied by `get_price_qualified_products`:

| Tier | `avg_order_price` | `eff_price` filter |
|---|---|---|
| `high` | < $80 | eff_price ≤ $90 |
| `medium` | $80–$119 | $80 ≤ eff_price ≤ $120 |
| `low` | ≥ $120 | any eff_price |

`eff_price` is always `sale_price` when `on_sale=true`, else `price`. This is pre-computed in Python before the LLM sees any product data.
