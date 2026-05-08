# Concepts and Flows

This document explains the data engineering and AI concepts behind every layer of the project, then traces each major data flow with real example values. It is the "why" companion to the code's "how".

## Event-Driven Architecture

### Why Events, Not Calls

A traditional system has services calling each other: the web app calls the recommendation service, which calls the profile service, which queries a database. This creates tight coupling — the caller must wait for the callee, and a failure anywhere breaks the chain.

An event-driven system inverts this. Each service publishes facts about what just happened:

```json
{"userid": 42, "event_type": "product_view", "productid": "NK-007", "category": "running", "ts": 1760000000000}
```

Downstream systems consume these facts at their own pace. The clickstream producer doesn't know Flink exists. Flink doesn't know agents exist. If the agent layer goes down, events accumulate in Kafka and are processed when it comes back up.

The key constraint: events describe **what happened**, not **what to do**. `product_view` is an event. `recommend_product` would be a command — that distinction matters as systems scale.

### Kafka as the Integration Fabric

In this project, nothing talks to anything else except through Kafka topics. The topology is:

```
Producer → Kafka Topic ← Consumer
```

This means adding a new consumer (fraud scorer, analytics engine, A/B test tracker) requires zero changes to producers or existing consumers.

---

## Kafka: Deep Concepts

### Topics, Partitions, and Offsets

A Kafka **topic** is a named, ordered, append-only log. Messages are immutable once written. A topic is split into **partitions**, and each partition is a separate ordered log.

An **offset** is the position of a message within a partition. It starts at 0 and increments monotonically. Consumers track which offset they've processed.

This project uses single-partition topics (suitable for local development and simulated load). Production systems partition by key to parallelize processing.

### Keys and Partitioning

Every Kafka message has an optional key. The key serves two purposes:

1. **Partitioning**: Messages with the same key always go to the same partition (via consistent hashing). This ensures that all events for user 42 land in the same partition and are processed in order.

2. **Log compaction**: On compacted topics, Kafka retains only the most recent message per key. If user 42's profile is updated 1000 times, a consumer catching up sees only the last version.

User event topics use `userid` as the key. Product event topics use `productid`.

### Consumer Groups and Offsets

A **consumer group** is a set of consumers that share work reading a topic. Kafka assigns each partition to exactly one consumer in the group. Consumers commit their offset after processing, so a restart picks up from where it left off.

Separate consumer groups read the same topic independently. In this project:
- Flink uses `flink-clickstream-group`, `flink-cart-group`, etc.
- The agent loop uses `agent-trigger-group`
- The Prometheus exporter uses its own group

Each group maintains its own committed offset. They don't interfere with each other.

### `auto_offset_reset`

When a consumer group has no committed offset (first run, or the offset expired), `auto_offset_reset` determines where to start:
- `earliest`: read from offset 0 — replay all history
- `latest`: start from the current end — only see new messages

The agent loop uses `latest`. The ChromaDB builder uses `earliest` (reads all product metadata from the start).

### Log Compaction

The `live-user-profile` and `live-product-profile` topics use `cleanup.policy=compact`. Kafka's log compaction process runs in the background and removes older messages with the same key, keeping only the most recent.

This prevents these topics from growing unboundedly. Without compaction, a topic that gets 1000 updates per user per day would accumulate millions of records. With compaction, each user has at most a few recent records.

The trade-off: consumers cannot replay full history from these topics. They can only read current state. That is fine for profiles — you want the most recent version, not the full changelog.

### Upsert Kafka Connector (Flink Output)

Flink's `upsert-kafka` connector:
1. Serializes the `PRIMARY KEY` into the Kafka message key
2. Serializes the full row into the Kafka message value
3. When Flink retracts a row (due to windowing), emits a tombstone (null value) for that key

Consumers that read this topic as a changelog-style table see inserts, updates, and deletes. The agent's `cached_latest_from_topic()` function reads all messages and keeps the last non-null value per key — which is the current state.

---

## Flink SQL: Deep Concepts

### Dynamic Tables

Flink SQL models a Kafka topic as a **dynamic table** — a table that never stops updating. SQL queries over dynamic tables produce dynamic results. A `SELECT COUNT(*) FROM clickstream GROUP BY userid` doesn't return once and stop; it keeps updating as new events arrive.

```
Time 0: clickstream has 0 rows
Time 1: user 42 views NK-007 → clickstream has 1 row
Time 2: user 42 searches    → clickstream has 2 rows
Time 3: user 99 views AD-001 → clickstream has 3 rows

COUNT(*) GROUP BY userid:
Time 1: {42: 1}
Time 2: {42: 2}
Time 3: {42: 2, 99: 1}
```

Each update to the count table is a changelog event that Flink can route to a sink.

### Event Time vs Processing Time

**Processing time** (`PROCTIME()`) is the wall clock time when the record arrives at Flink. It is simple but inconsistent — a 5-second network delay would make a late-arriving event appear to be in a different window.

**Event time** uses the timestamp embedded in the event (`ts` field in clickstream). This is when the event actually occurred, regardless of network delay. Flink uses a **watermark** to know when a window is complete:

```sql
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

This means Flink assumes no event will arrive more than 5 seconds late. When the watermark advances past a window boundary, the window closes and emits its result.

`clickstream` uses event time because user intent should be computed based on when the user acted, not when the message was processed. `cart_updates`, `inventory`, and `product_metadata` use `PROCTIME()` — their aggregations don't depend on precise event ordering.

### HOP Windows

A **HOP** (hopping/sliding) window is defined by two parameters:
- **slide** (advance): how often a new window starts
- **size**: how long each window covers

```sql
HOP(TABLE clickstream, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)
```

This creates overlapping windows: one window covers minutes 0–14, the next covers minutes 1–15, the next 2–16, and so on. Every minute, Flink emits updated counts for the window that closed.

A user who was active in the last 15 minutes will appear in current window results. A user who last acted 20 minutes ago won't. This is what makes the intent window "live" — it automatically reflects recent activity.

### Regular vs Temporal Joins

The user profile `INSERT INTO` joins HOP-windowed results with the `user_orders` view (which is a plain `GROUP BY`, not windowed):

```sql
FROM user_intent_totals t
LEFT JOIN user_orders o ON t.userid = o.userid
```

This is a **regular join** — Flink joins the latest cumulative count from `user_orders` onto each window row from `user_intent_totals`. The result is: for every window output (current 15-minute behavior), attach the user's full order history.

The product profile join works similarly: each inventory update triggers a join with the current `product_orders` count and latest `product_metadata`.

### The `category_rank = 1` Pattern

Determining the user's most-active category requires:
1. Count events per category per user per window (`user_category_interest`)
2. Rank categories by count within each (user, window) group (`ranked_user_category_interest`)
3. Filter to rank=1 to get the top category

```sql
ROW_NUMBER() OVER (
    PARTITION BY userid, window_end
    ORDER BY active_interest_events DESC, category ASC
) AS category_rank
```

The `category ASC` secondary sort provides deterministic tie-breaking — if a user has equal activity in "running" and "lifestyle", "lifestyle" wins alphabetically. This is arbitrary but consistent.

### Price Sensitivity Formula

```sql
CASE
    WHEN AVG(price) < 80  THEN 'high'
    WHEN AVG(price) < 120 THEN 'medium'
    ELSE 'low'
END AS price_sensitivity
```

This uses `AVG(price)` from `cart_updates`, which is the user's average transaction price. "High sensitivity" means the user typically spends under $80 — they are price-conscious. The label feels counter-intuitive: high sensitivity = budget, low sensitivity = premium. The naming follows the convention "how much does price affect their decisions?"

---

## Vector Search: Deep Concepts

### Why Vector Search Exists

The agent needs to find relevant products for a user. A SQL approach would be: `WHERE category = 'running' AND price <= 90`. This works for hard constraints but misses semantic intent.

A user searching for "plush long-distance trainer" and a user searching for "cushioned marathon shoe" have the same intent, but neither query exactly matches a column value. Vector search solves this.

### What an Embedding Is

An embedding is a transformation of text (or any data) into a fixed-length vector of numbers. The `all-MiniLM-L6-v2` model maps any sentence to a 384-dimensional vector.

The model is trained so that semantically similar sentences produce vectors that are close to each other in that 384-dimensional space. "Cushioned running shoe" and "padded daily trainer" produce vectors that point in nearly the same direction. "Running shoe" and "leather dress shoe" produce vectors that point in very different directions.

### Cosine Similarity

For two vectors A and B:
```
cosine_similarity = (A · B) / (|A| × |B|)
```

For unit-normalized vectors (as produced by `all-MiniLM-L6-v2`), this simplifies to the dot product. Values range from -1 to 1:
- 1.0: identical direction (identical meaning)
- 0.0: perpendicular (unrelated)
- -1.0: opposite directions (opposite meaning)

ChromaDB stores `cosine_distance = 1 - cosine_similarity`. The query result's `distances` field contains these distances. The code converts back: `similarity_score = 1 - distance`.

Typical scores in this catalog:
- Strong match: 0.55–0.65
- Moderate match: 0.45–0.55
- Weak match: 0.35–0.45

### Document Enrichment

The raw product description "Nike Free Run 5.0: Lightweight and flexible daily trainer" embeds only the product's textual meaning. But we want "affordable running shoe" to rank differently from "premium running shoe". So documents are enriched:

```
"Nike Free Run 5.0: Lightweight and flexible daily trainer for everyday running.
Category: running. Price tier: mid-range. On sale at $79.99 (from $99.99).
Attributes: cushioned, breathable, daily trainer."
```

Now "budget running shoe" will prefer documents that contain "budget" or "sale" language, and "premium marathon" will prefer documents without sale language and with premium-tier pricing context.

### Category Pre-Filtering

ChromaDB supports `where` clauses on stored metadata:

```python
where = {"category": {"$eq": "running"}}
results = collection.query(query_texts=[query], n_results=5, where=where)
```

This filters to only running shoes before similarity ranking. Without this, a broad query like "cushioned comfortable shoe" might return a hiking shoe or lifestyle sneaker that happens to embed close to the query. Category filtering ensures the agent sees only shoes from the user's current interest category.

### The Module-Level Cache

```python
_collection = None

def _get_collection():
    global _collection
    if _collection is None:
        _collection = _build_collection()
    return _collection
```

The ChromaDB collection is built once per process. Building it takes several seconds (reading two Kafka topics, embedding 30 products). The module-level singleton ensures this only happens once regardless of how many tool calls are made.

The downside: if product metadata changes during a long-running process, the index won't reflect it until the process restarts. For this learning project, that is acceptable.

---

## CrewAI Agent Framework: Deep Concepts

### The Tool Calling Loop

When `crew.kickoff()` runs, the following cycle happens inside the CrewAI/LLM layer:

1. **System prompt**: CrewAI constructs a prompt containing the agent's role, goal, backstory, and available tool schemas (names + descriptions + argument schemas)
2. **Task description**: The task's `description` field is appended
3. **LLM inference**: The model generates either a tool call request or a final answer
4. **Tool execution**: If a tool call, CrewAI executes the Python function with the provided arguments and appends the result to the conversation
5. **Repeat**: Steps 3–4 repeat until the model produces a final answer
6. **Output**: The final answer is returned from `crew.kickoff()`

This loop is why verbose mode shows multiple tool calls followed by reasoning followed by a final answer. The model is literally deciding what to check next based on what it has already learned.

### Tool Schema

Every `@tool`-decorated function gets a schema automatically generated from its docstring and type annotations:

```python
@tool("Find Similar Products")
def find_similar_products(query: str, category: str = "") -> str:
    """Semantic search over the product catalog. ..."""
```

CrewAI passes this to the LLM as: `{"name": "Find Similar Products", "description": "...", "parameters": {"query": {"type": "string"}, "category": {"type": "string", "default": ""}}}`.

The LLM uses this schema to decide when and how to call the tool. A well-written docstring directly improves recommendation quality by giving the model better guidance.

### Task Prompts as Guardrails

The task `description` is not just instructions — it is a guardrail. The more specific the instructions, the less the model needs to improvise:

- "Call `Find Similar Products` with TWO arguments: query AND category" prevents single-argument calls
- "Call `Get Price Qualified Products` with THREE arguments" prevents the model from trying to filter manually
- "ONLY use productids that appeared in the tool results. Never invent a product." prevents hallucination
- "YOUR ENTIRE RESPONSE MUST BE EXACTLY THESE 5 LINES" prevents markdown and explanatory text

Every instruction in the task prompt exists because an earlier version of the system had a bug that the instruction was added to fix.

### Temperature Setting

Both agents use `temperature=0.2`. Temperature controls how "creative" vs "focused" the model's outputs are:
- Temperature 0.0: maximally deterministic, always picks highest-probability token
- Temperature 1.0: more variety, but also more errors

For tool calling and structured output, low temperature is better — you want the model to consistently call the right tool with the right arguments, not to explore alternatives.

---

## End-to-End Flows

### Flow 1: A New User Action → Updated Profile

```
1. User 42 visits the website and searches for "trail running shoes"

2. clickstream_producer.py publishes:
   topic: shoe-clickstream
   key:   "42"
   value: {userid: 42, event_type: "search", category: "running",
           query: "trail running shoes", ts: 1760000100000}

3. Flink reads this from shoe-clickstream (it was waiting for new events)

4. The HOP window [10:00, 10:15] for user 42 now has one more search event

5. user_intent_totals view emits:
   {userid: 42, window_end: 2026-05-08 10:01, recent_searches: 1, recent_cart_adds: 0}

6. The JOIN with user_orders (from earlier purchases) returns:
   {total_orders: 24, avg_order_price: 97.50, price_sensitivity: "medium"}

7. The JOIN with ranked_user_category_interest returns:
   {active_interest_category: "running", active_interest_events: 1}

8. Flink emits to live-user-profile:
   key:   {"userid": 42}
   value: {userid: 42, recent_searches: 1, recent_cart_adds: 0,
           active_interest_category: "running", total_orders: 24,
           avg_order_price: 97.50, price_sensitivity: "medium",
           updated_at: "2026-05-08 10:01:00.000"}

9. main.py consumer receives this message:
   price_sensitivity = "medium" (not "unknown") → trigger personalization agent
```

### Flow 2: Personalization Agent Execution

```
Input: userid=42, price_sensitivity="medium", avg_order_price=$97.50,
       active_interest_category="running", recent_searches=1

Step 1 — Build query (LLM reasoning):
  Category: running, avg_order_price: $97.50, recent_searches: 1 (moderate)
  → query: "comfortable neutral daily running shoe"

Step 2 — find_similar_products(query="comfortable neutral daily running shoe", category="running")
  ChromaDB embeds query, computes cosine distance to all running shoes
  Returns top 5:
    NK-007: 0.518  (Nike Free Run 5.0, mid-range, on sale $79.99)
    NK-003: 0.517  (Nike React Infinity, mid-range, on sale $99.99)
    NK-002: 0.494  (Nike Pegasus 40, mid-range, $119.99)
    NK-001: 0.481  (Nike Air Zoom Tempo, mid-range, on sale $89.99)
    PU-003: 0.467  (Puma Velocity Nitro, mid-range, $109.99)

Step 3 — get_price_qualified_products(price_sensitivity="medium", avg_order_price=97.50, category="running")
  Filter: 80 ≤ eff_price ≤ 120
  NK-007 eff_price=79.99 → EXCLUDED (below $80 floor)
  NK-003 eff_price=99.99 → included (closest to $97.50, Δ=$2.49)
  NK-002 eff_price=119.99 → included (Δ=$22.49)
  NK-001 eff_price=89.99 → included (Δ=$7.51)
  PU-003 eff_price=109.99 → included (Δ=$12.49)
  Returns sorted by |eff_price - avg_order_price|:
    NK-003 (Δ=$2.49), NK-001 (Δ=$7.51), PU-003 (Δ=$12.49), NK-002 (Δ=$22.49)

Step 4 — LLM cross-references:
  Similarity results: NK-007 (0.518), NK-003 (0.517), NK-002 (0.494), NK-001 (0.481), PU-003 (0.467)
  Qualified products: NK-003, NK-001, PU-003, NK-002
  Overlap with highest similarity: NK-003 (0.517)
  Runner-up: NK-001 (0.481) — 0.036 gap, not within 0.05 tiebreaker threshold
  → Pick NK-003

Step 5 — LLM generates output:
  "Product: Nike React Infinity (NK-003)
   Price: $129.99 (sale $99.99)
   Live signal: running, 1 recent search
   Why this user: medium sensitivity, avg $97.50 order price, daily running interest
   Stock: 16 units, low trend"

Step 6 — write_recommendation(userid=42, recommendation=...) → recommendations topic
```

### Flow 3: Merchandising Agent (Deterministic)

```
Every 5 minutes, main.py triggers run_merchandising()

1. rank_promotion_candidates() reads latest product profiles
2. Sorts by tuple: (stock_trend=="low", stock≤20, demand_score, on_sale, avg_rating)

Example sort for top 3:
  PU-001: (1, 0, 1.84, 1, 4.1)  ← stock_trend=low, demand high, on sale
  VN-001: (1, 0, 1.96, 1, 4.2)  ← stock_trend=low, demand high, on sale
  NK-005: (0, 1, 1.87, 1, 4.3)  ← stock≤20, demand high, on sale

  Sorted descending: VN-001 (1,0,1.96,...), PU-001 (1,0,1.84,...), NK-005 (0,1,1.87,...)

3. format_promotion_recommendation() generates output:
  "Top 3 products to promote right now:
   1. Vans Old Skool (VN-001)
      Reason: stock=5, stock_trend=low, demand_score=1.96, sale price $54.99
      Recommended channel: email
      Expected impact: prioritize live demand while inventory and pricing signals are current

   2. Puma Suede Classic (PU-001)
      ..."

4. write_recommendation(userid=0, agent_type="merchandising", ...) → recommendations topic
```

### Flow 4: Langfuse Trace

```
Langfuse receives traces only when LANGFUSE_PUBLIC_KEY + LANGFUSE_SECRET_KEY are set.

1. crew.py: trace_context("personalization-agent-run", run_id="personalization-a3f8b2c1", user_id=42)
   → Creates a Langfuse trace object
   → Stores in contextvars.ContextVar so nested calls can attach spans to it

2. Inside the agent loop (tool calls):
   Each @tool function calls trace_span() before returning:
   
   trace_span("kafka.get_user_profile",
     input={"topic": "live-user-profile", "userid": 42},
     output={...profile...},
     start_time=t0   ← captured before Kafka read
   )
   
   This creates a span on the active trace with:
   - start_time: when the Kafka read began
   - end_time: when trace_span() was called
   - latency_ms: end - start

3. crew.py: trace_span("crew-final-answer", output=str(result))
   → Final span capturing the LLM's recommendation text

4. trace_context.__exit__: flush_langfuse()
   → Ensures all spans are sent before the context exits
```

### Flow 5: Prometheus Metrics Update

```
kafka_exporter.py runs every 30 seconds:

1. Reads live-user-profile → counts users by sensitivity, category
   → shoe_users_total{price_sensitivity="medium"} = 34
   → shoe_users_by_category{category="running"} = 36

2. Reads live-product-profile → stock, demand, sale status per product
   → shoe_product_stock{productid="NK-007", name="Nike Free Run 5.0"} = 53
   → shoe_product_demand_score{productid="NK-007", ...} = 1.62
   → shoe_products_low_stock = 3  (products with stock < 20)

3. Reads recommendations → counts by agent type
   → shoe_recommendations_total{agent_type="personalization"} = 847

4. Exposes all metrics at http://localhost:8888/metrics

5. Prometheus (in Docker) scrapes http://host.docker.internal:8888/metrics
   → Stores time-series samples

6. Grafana queries Prometheus
   → Plots stock trends, demand heatmaps, sensitivity distribution over time
```

---

## Key Concepts Summary

### Kafka

| Concept | What it means in this project |
|---|---|
| Topic | Named event stream: `shoe-clickstream`, `cart-updates`, `live-user-profile`, etc. |
| Key | Per-event routing key: `userid` for user events, `productid` for product events |
| Offset | Position in a partition; consumer groups commit offsets to track progress |
| Consumer group | Independent reader of a topic; multiple groups can read the same data |
| Log compaction | Keeps only the latest message per key; used for profile topics |
| Upsert sink | Kafka output that overwrites previous value for the same key |

### Flink SQL

| Concept | What it means in this project |
|---|---|
| Dynamic table | A Kafka topic viewed as a continuously changing relational table |
| Source table | `CREATE TABLE` mapping a Kafka topic to a schema |
| View | Reusable streaming query; not stored, re-evaluated on each `INSERT INTO` |
| HOP window | Sliding window: slide=1min, size=15min produces rolling 15-min activity counts |
| Watermark | Tells Flink how late events can arrive; clickstream uses 5-second watermark |
| Event time | Uses producer `ts` timestamp for window assignment; more accurate than wall clock |
| Processing time | Wall clock at Flink arrival; simpler but window boundaries drift with latency |
| upsert-kafka | Sink connector that writes changelog events (insert/update/delete) to Kafka |

### Vector Search

| Concept | What it means in this project |
|---|---|
| Embedding | 384-dimensional vector from `all-MiniLM-L6-v2`; similar text → similar vectors |
| Cosine distance | Correct metric for normalized embeddings; lower = more similar |
| L2 distance | Wrong metric for unit vectors; ChromaDB default — overridden to cosine here |
| Where filter | Pre-filter by category before similarity ranking; prevents cross-category matches |
| Document enrichment | Adding price tier, category, sale status to embedding text improves relevance |
| Module cache | Index built once per process; subsequent calls use the cached collection |

### Agents

| Concept | What it means in this project |
|---|---|
| Tool | Python function the LLM can call; returns text that the LLM reads as context |
| Task | Natural-language instruction with expected output format |
| Crew | Set of agents + tasks; `Process.sequential` runs them in order |
| Tool calling loop | LLM ↔ tool execution cycle until the model produces a final answer |
| Deterministic guardrail | Price filtering done in Python, not LLM reasoning, for correctness |
| Demand tiebreaker | When top candidates within 0.05 similarity, pick higher demand_score |
