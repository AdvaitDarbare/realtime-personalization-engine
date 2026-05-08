# Agents Deep Dive

This document covers the agent layer in full: the CrewAI framework, every tool, the personalization task logic, the merchandising ranking logic, LLM configuration, observability, and the design decisions behind each choice.

## File Structure

```
agents/
├── main.py                  # Entry point: Kafka consumer loop, triggers agents
├── crew.py                  # CrewAI crew assembly and Langfuse trace context
├── observability.py         # Langfuse client, trace/span helpers
├── config/
│   └── agents.py            # LLM config, agent definitions, tool assignment
├── tasks/
│   └── tasks.py             # Task prompts for personalization and merchandising
├── tools/
│   ├── kafka_tools.py       # All Kafka-backed tools + merchandising logic
│   └── vector_tools.py      # ChromaDB index build + find_similar_products tool
└── smoke_test.py            # Health checks for all system layers
```

---

## CrewAI Framework

### Agent

```python
Agent(
    role="Personalization Specialist",
    goal="Recommend the perfect shoe...",
    backstory="You are an expert in real-time personalization...",
    tools=[find_similar_products, get_user_profile, get_price_qualified_products],
    llm=agent_llm,
    verbose=True
)
```

The `role`, `goal`, and `backstory` go into the LLM's system prompt. They shape the model's persona. The `tools` list determines what the model can call. `verbose=True` prints every tool call and reasoning step to stdout — useful for debugging.

### Task

```python
Task(
    description="Analyze the live profile for user {userid} and recommend...",
    expected_output="Exactly five plain-text lines with no markdown...",
    agent=personalization_agent
)
```

The `description` is the full instruction to the agent. It specifies which tools to call, in what order, with what arguments, and how to select a result. The `expected_output` hints to the LLM what the final answer format should look like.

Every line of the task description exists because an earlier version had a failure that the instruction was written to prevent:
- "Call `Get Price Qualified Products`. You MUST call this tool. Do not skip it." — agent was skipping it
- "ONLY use productids that appeared in the tool results." — agent hallucinated product IDs with small LLMs
- "YOUR ENTIRE RESPONSE MUST BE EXACTLY THESE 5 LINES." — agent was wrapping output in markdown headers

### Crew and Process

```python
Crew(
    agents=[personalization_agent],
    tasks=[create_personalization_task(userid)],
    process=Process.sequential,
    verbose=True
)
result = crew.kickoff()
```

`Process.sequential` runs each task in order and passes the output of one task as context to the next. For single-agent runs (personalization, merchandising), this is equivalent to running the task directly. For `run_full_crew()`, it chains personalization then merchandising.

`crew.kickoff()` blocks until all tasks are complete and returns the string output of the final task.

### The Tool Calling Loop (internals)

When `kickoff()` runs, CrewAI constructs a prompt containing:
1. System context: role, goal, backstory, available tool schemas
2. Task description and expected output

The LLM produces one of two things:
- A tool call: `{"tool": "get_user_profile", "arguments": {"userid": 42}}`
- A final answer: the actual recommendation text

If a tool call, CrewAI executes the Python function, appends the result to the conversation as a "tool result" message, and calls the LLM again. This repeats until the LLM produces a final answer.

Temperature=0.2 is used throughout. Low temperature means the model takes fewer creative risks — it will consistently call the right tool with the right arguments rather than exploring alternatives.

---

## Personalization Agent

### Tool 1: `Get Live User Profile`

```python
@tool("Get Live User Profile")
def get_user_profile(userid: int) -> str:
```

**What it does:** Reads the latest user profile from the `live-user-profile` Kafka topic.

**Progressive lookback:** The function tries three scan sizes: 20,000 → 500,000 → 5,000,000 messages. This handles the case where a user's profile is old (deep in the topic) without always scanning 5M messages.

```python
for lookback in (20000, 500000, 5_000_000):
    profiles = cached_latest_from_topic('live-user-profile', 'userid', lookback)
    profile = profiles.get(userid)
    if profile:
        break
```

**Topic cache:** `cached_latest_from_topic()` caches results for 10 seconds. Parallel tool calls for different users reuse the same Kafka scan, reducing I/O.

**Returns:** JSON string of the user profile, or `"No profile found for user {userid}"`.

**Example output:**
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
  "avg_order_price": 97.50,
  "price_sensitivity": "medium",
  "updated_at": "2026-05-08 10:01:00.000"
}
```

### Tool 2: `Find Similar Products`

```python
@tool("Find Similar Products")
def find_similar_products(query: str, category: str = "") -> str:
```

**What it does:** Queries ChromaDB with a natural-language description and returns the 5 most similar products by cosine similarity.

**Category filter:** If `category` is provided, only products in that category are searched. The filter uses ChromaDB metadata:
```python
where = {"category": {"$eq": category}} if category else None
results = collection.query(query_texts=[query], n_results=5, where=where)
```

**Fallback:** If the where-clause returns zero results (no products in that category), the tool falls back to an unfiltered search. This handles edge cases like the single racing, hiking, or football shoe.

**Returns:** JSON array of match objects sorted by `similarity_score` descending.

**Example output:**
```json
[
  {"productid": "NK-003", "name": "Nike React Infinity", "category": "running",
   "price_tier": "mid-range", "similarity_score": 0.517},
  {"productid": "NK-007", "name": "Nike Free Run 5.0", "category": "running",
   "price_tier": "mid-range", "similarity_score": 0.518},
  ...
]
```

**Query construction guidance (from the task prompt):**

The task instructs the agent to build a 4–8 word query using multiple user signals:
```
running + avg_order_price $70 + many cart_adds  → "budget everyday running shoe active shopper"
running + avg_order_price $100 + low sensitivity → "premium cushioned marathon daily trainer"
lifestyle + avg_order_price $65 + many searches → "trendy street casual sneaker popular"
lifestyle + avg_order_price $75 + many cart_adds → "versatile canvas classic retro shoe"
```

Varying the query per user is critical for diversity. Without varied queries, all users in the same category+sensitivity bucket would get the same top result.

### Tool 3: `Get Price Qualified Products`

```python
@tool("Get Price Qualified Products")
def get_price_qualified_products(price_sensitivity: str, avg_order_price: float, category: str = "") -> str:
```

**What it does:** Returns only products that pass the price sensitivity filter. The filtering is done entirely in Python, never by the LLM.

**Price tier logic:**
```python
if price_sensitivity == "high" and eff_price <= 90:
    qualified.append(p)
elif price_sensitivity == "medium" and 80 <= eff_price <= 120:
    qualified.append(p)
elif price_sensitivity == "low":
    qualified.append(p)
```

`eff_price` is pre-computed on each product: `sale_price if on_sale else price`. This is already stored in the `eff_price` field added by `latest_product_profiles()`.

**Why this tool exists instead of instructions:** The LLM was consistently failing to apply the $80 floor for medium sensitivity. Given products with `on_sale=true, sale_price=79.99, price=99.99`, the model would see "mid-range" in the price tier (based on $99.99) and conclude the product qualifies for medium sensitivity ($80–$120). Moving the filter into Python eliminates this error class entirely.

**Sorting:** Products are sorted by `|eff_price - avg_order_price|` ascending, then `demand_score` descending. This means the first product in the result is the one closest in price to what the user typically spends. This nudges recommendations toward familiar price points.

**Returns:** JSON array of qualified products.

**Stock filter:** Products with `stock = 0` are excluded entirely — no point recommending an out-of-stock item.

### Selection Logic (Task Prompt Step 5)

After the agent has similarity results (step 3) and qualified products (step 4), it does a cross-reference:

```
1. Find products that appear in BOTH lists
2. Among the overlapping products, pick the one with the highest similarity_score from step 3
3. TIEBREAKER: if two products are within 0.05 similarity, pick the one with higher demand_score
4. If no overlap: pick the highest demand_score product from the qualified list
```

**Why tiebreaker on demand_score?** Products with near-identical similarity scores (within 0.05) are semantically interchangeable for this user's query. In that case, the product with more current purchasing activity is the better business choice — it is selling well right now, which might indicate fashion momentum, better product quality, or promotional visibility.

**Why the fallback to demand_score?** If the vector results have zero overlap with the qualified products (rare, but possible when all high-similarity products are sold out or wrong price tier), the agent needs something to fall back on. Demand score is the most live signal available.

### Output Format

The task requires exactly 5 plain-text lines with no markdown:

```
Product: Nike React Infinity (NK-003)
Price: $129.99 (sale $99.99)
Live signal: running, 1 recent search
Why this user: medium sensitivity, avg $97.50 order price, daily running interest
Stock: 16 units, low trend
```

The strict format is enforced because:
1. The output is written to Kafka as-is — Grafana and downstream consumers need a predictable shape
2. LLMs tend to add explanatory paragraphs, headers, or markdown bullets unless told explicitly not to
3. The `CRITICAL — YOUR ENTIRE RESPONSE MUST BE EXACTLY THESE 5 LINES` instruction + caps dramatically improves format compliance

---

## Merchandising Agent

### Architecture: Deterministic First

The merchandising agent is architecturally different from the personalization agent. Ranking products for promotion is a well-defined algorithm — there is a correct answer and no ambiguity. Using an LLM for this would only add latency and risk of incorrect reasoning.

`build_merchandising_recommendation()` in `kafka_tools.py` does the full ranking in Python and produces the formatted output. The LLM (via CrewAI) is still invoked as a formality, but the actual decision is already made.

### Ranking Algorithm

```python
def promotion_urgency(product: dict) -> tuple:
    return (
        1 if stock_trend == "low" else 0,
        1 if stock <= 20 else 0,
        round(demand_score, 4),
        1 if on_sale else 0,
        product.get("avg_rating") or 0,
    )

ranked = sorted(products, key=promotion_urgency, reverse=True)
```

Tuples compare element-by-element. So a product must beat another on field 1 before field 2 matters. The priority hierarchy is:

1. **stock_trend = "low"** — The most urgent signal. A product trending toward zero needs promotion before it sells out.
2. **stock ≤ 20** — Absolute scarcity. Even if trend is "medium", 18 units means promotion now.
3. **demand_score** — Products currently selling well should be amplified.
4. **on_sale** — Sale products need visibility to justify the discount.
5. **avg_rating** — All else equal, higher-rated products promote better.

### Channel Logic

```python
channel = "push_notification" if stock_trend == "low" else "homepage_banner"
if on_sale:
    channel = "email"
```

- Email = sale awareness (customers who opted in to deal emails)
- Push notification = urgency (low stock, act now)
- Homepage banner = general visibility (moderate demand, no special condition)

### Tools Available

The merchandising agent has access to `get_all_products` and `get_active_users`. These are available if the task prompt wants to reference them, but the pre-computed recommendation in `build_merchandising_recommendation()` doesn't require the agent to call them — the ranking is already done.

---

## Main Loop (`main.py`)

```python
consumer = KafkaConsumer(
    'live-user-profile',
    group_id='agent-trigger-group',
    auto_offset_reset='latest',
    consumer_timeout_ms=60000,
    max_poll_interval_ms=900000,
    max_poll_records=1,
)
```

Key configuration choices:

- `group_id='agent-trigger-group'`: Uses committed offsets between restarts. Each process restart continues from where the previous one left off.
- `auto_offset_reset='latest'`: First run starts from the current end of the topic. Only new profile updates trigger agents.
- `max_poll_records=1`: Processes one profile at a time. This prevents the consumer from pulling ahead while an agent run takes 15–30 seconds.
- `max_poll_interval_ms=900000`: Gives up to 15 minutes between polls before Kafka kicks the consumer out of the group. Necessary because each agent run takes ~30 seconds, and a backlog could accumulate.
- `consumer_timeout_ms=60000`: If no new messages arrive for 60 seconds, the `for message in consumer` loop ends (and the process exits). This is a design choice: the loop exits if idle rather than blocking forever.

### Trigger Condition

```python
price_sensitivity = profile.get('price_sensitivity', 'unknown')
if price_sensitivity != 'unknown' and userid not in processed_users:
    run_personalization(userid)
    processed_users.add(userid)
```

`price_sensitivity = 'unknown'` means Flink hasn't seen any cart events for this user yet (post-restart cold start). Triggering on an 'unknown' profile would give the agent no price guidance, so these are skipped until the profile has real data.

`processed_users` is an in-memory set that prevents re-running an agent for the same user within one process lifetime. On restart, it resets to empty — all users seen in the first wave of profile updates are candidates again.

### Merchandising Cadence

```python
if current_time - last_merch_run > 300:
    run_merchandising()
    last_merch_run = current_time
```

Merchandising runs every 5 minutes, triggered by the arrival of any Kafka message (not a separate timer). If the profile topic is idle, merchandising also pauses. This is acceptable — if no users are active, product signals aren't changing either.

---

## Observability (`observability.py`)

### Langfuse Client

```python
_langfuse = Langfuse(
    public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
    secret_key=os.environ["LANGFUSE_SECRET_KEY"],
    host=LANGFUSE_HOST,
)
```

The client is instantiated once (module-level singleton, lazy-initialized). If `LANGFUSE_PUBLIC_KEY` or `LANGFUSE_SECRET_KEY` are not set, Langfuse is silently disabled — all trace/span calls become no-ops.

### Trace Context

```python
with trace_context("personalization-agent-run", run_id=run_id, user_id=userid):
    result = crew.kickoff()
```

`trace_context()` is a context manager that:
1. Creates a Langfuse trace with the given name, run_id, and user_id
2. Stores the trace object in a `contextvars.ContextVar` — a thread-safe context variable
3. On exit, calls `flush_langfuse()` to ensure all spans are sent before the context exits

Using `contextvars.ContextVar` means the trace is accessible from any code called within the `with` block, without needing to pass it as a parameter. Tool functions call `current_trace()` to get the active trace.

### Span Recording

```python
def trace_span(name, *, input=None, output=None, metadata=None, start_time=None):
    t0 = start_time if start_time is not None else time.time()
    t1 = time.time()
    latency_ms = round((t1 - t0) * 1000, 2)
    span = trace.span(
        name=name,
        start_time=datetime.fromtimestamp(t0, tz=timezone.utc),
        input=safe_json(input),
        metadata={**(metadata or {}), "latency_ms": latency_ms},
    )
    span.end(...)
```

The `start_time` parameter is critical. Without it, a span would show 0ms latency because `trace_span()` is called *after* the tool function has already done its work. By capturing `t0 = time.time()` at the start of each tool function and passing it to `trace_span()`, the span accurately reflects how long the Kafka read or vector query took.

### `safe_json()`

Langfuse spans accept JSON-serializable objects. Tool outputs can be large. `safe_json()` handles:
- Strings that contain JSON (parsed and returned as the parsed object)
- Large objects (truncated at 12,000 characters to keep traces readable)
- Non-JSON-serializable objects (converted to string)

---

## LLM Configuration

### OpenAI (default)

```bash
AGENT_LLM_PROVIDER=openai
AGENT_LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...
```

`gpt-4o-mini` is the recommended default. It has excellent tool-calling reliability, follows multi-step task instructions, and respects output format constraints. Cost: ~$0.15/1M input tokens, ~$0.60/1M output tokens. A typical personalization run uses ~2,000–3,000 tokens.

### Ollama (local)

```bash
AGENT_LLM_PROVIDER=ollama
AGENT_LLM_MODEL=qwen3.5:4b
AGENT_LLM_BASE_URL=http://localhost:11434
```

Local models work but require careful selection:
- Models under 4B parameters frequently hallucinate product IDs or skip tool calls
- Models that don't support tool calling natively (function calling format) will fail silently
- `gemma2:2b`, `qwen3:1.7b` tested and unreliable for this task
- `qwen3.5:4b` and larger are more reliable but still occasionally fail format compliance

For production-quality recommendations, use `gpt-4o-mini` or a locally hosted model ≥ 7B with confirmed tool-calling support (e.g., `llama3.1:8b`, `mistral:7b-instruct`).

### `build_llm()` in agents.py

```python
def build_llm() -> LLM:
    provider = os.getenv("AGENT_LLM_PROVIDER", "ollama")
    model = os.getenv("AGENT_LLM_MODEL", "qwen3.5:4b")
    base_url = os.getenv("AGENT_LLM_BASE_URL", "http://localhost:11434")

    if provider == "ollama":
        return LLM(model=model, provider="ollama", base_url=base_url, temperature=0.2, timeout=120)
    
    return LLM(model=model, provider=provider, temperature=0.2, timeout=120)
```

For `provider=openai`, CrewAI uses LiteLLM under the hood to route to OpenAI's API. No special handling needed — just set `OPENAI_API_KEY`.

For `provider=ollama`, `base_url` points to the Ollama server (local by default, or a remote host).

---

## Data Flow Through Agent Tools

```
live-user-profile (Kafka)
    │
    ▼ get_user_profile(userid)
    │   → reads topic, returns JSON
    │
    ├── active_interest_category ──────────────────────────────────────────┐
    │                                                                      │
    ├── price_sensitivity + avg_order_price ──────────┐                   │
    │                                                  │                   │
    ▼                                                  │                   │
find_similar_products(query, category)                 │                   │
    │   ChromaDB cosine search, category filter        │                   │
    │   → top-5 [(productid, similarity_score), ...]  │                   │
    │                                                  │                   │
    ▼                                                  ▼                   ▼
get_price_qualified_products(sensitivity, avg, category)
    │   Python price filter (eff_price = sale_price or price)
    │   Returns qualified products sorted by |eff_price - avg_order_price|
    │
    ▼
LLM cross-reference:
    Find overlap between similarity results and qualified products
    Pick highest similarity_score
    Tiebreak on demand_score if within 0.05
    │
    ▼
Output: 5-line recommendation text
    │
    ▼ write_recommendation(userid, recommendation, agent_type)
recommendations (Kafka)
```

---

## Design Evolution

The personalization agent went through several iterations. Understanding the progression helps explain why the current design is the way it is.

**v1: LLM picks from all 30 products**
The task told the agent to call `get_all_products` and pick the best match. The LLM had to reason about 30 products in one prompt. Results were inconsistent and slow.

**v2: Vector search narrows candidates**
`find_similar_products` was added. The LLM now got 5 pre-ranked candidates from ChromaDB. Quality improved significantly — semantically relevant products were in the candidate set.

**v3: Category filter on vector search**
Without a category filter, "lifestyle shoe" queries occasionally returned running shoes with slightly better embeddings. Adding `where={"category": "running"}` kept category-appropriate results.

**v4: Cosine distance fix**
ChromaDB defaulted to L2 distance. Cosine distance is correct for normalized sentence embeddings. Switching raised similarity scores from ~0.22 to ~0.48–0.60 and improved result relevance.

**v5: Enriched product documents**
Adding "Category: running. Price tier: mid-range. On sale at $79.99." to each document made queries like "budget running shoe" differentiate budget from premium within a category.

**v6: Price-qualified tool**
The LLM was consistently failing to exclude products with `eff_price < $80` from medium-sensitivity users. The root cause: it saw `price=$99.99` (mid-range) and `sale_price=$79.99` but reasoned from the catalog price, not the effective price. Moving the filter to `get_price_qualified_products` (Python code) eliminated this error class entirely.

**v7: Varied queries**
All users in the same category+sensitivity bucket were generating the same query ("affordable casual lifestyle sneaker budget") and getting the same top result. The task prompt was updated to encourage queries that vary by `avg_order_price` and behavior signals, producing more diverse recommendations.
