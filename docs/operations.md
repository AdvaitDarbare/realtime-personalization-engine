# Operations Runbook

Complete guide for starting, verifying, and debugging every layer of the real-time shoe personalization engine. Follow sections in order for a first run; jump directly to a debugging section when a specific layer is broken.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Variables](#environment-variables)
3. [Start Infrastructure (Docker)](#start-infrastructure-docker)
4. [Submit Flink SQL Jobs](#submit-flink-sql-jobs)
5. [Start Data Producers](#start-data-producers)
6. [Build Vector Index](#build-vector-index)
7. [Start Metrics Exporter](#start-metrics-exporter)
8. [Run Agent Loop](#run-agent-loop)
9. [Observability Surfaces](#observability-surfaces)
10. [Smoke Test](#smoke-test)
11. [Health Checks by Layer](#health-checks-by-layer)
12. [Debugging by Layer](#debugging-by-layer)
13. [Known Issues and Workarounds](#known-issues-and-workarounds)
14. [Teardown](#teardown)

---

## Prerequisites

**Required:**
- Docker Desktop (running, with at least 6 GB memory allocated)
- Python 3.11
- An OpenAI API key (`sk-...`)

**Optional:**
- A Langfuse account (local Docker instance is included — no cloud signup needed)

**Install Python dependencies:**

```bash
cd /path/to/shoe-personalization
python3.11 -m venv agents/venv
source agents/venv/bin/activate
pip install -r requirements.txt
```

The virtual environment lives at `agents/venv/`. All Python commands in this doc assume it is activated.

---

## Environment Variables

All configuration is via environment variables. The agent reads them at startup; no config files are parsed.

**Create a `.env` file in the repo root:**

```bash
cat > .env <<EOF
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# LLM — OpenAI
AGENT_LLM_PROVIDER=openai
AGENT_LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...

# Langfuse observability (local Docker instance)
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_HOST=http://localhost:3001
EOF
```

**Full variable reference:**

| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `localhost:9092` | Kafka broker address used by all producers, consumers, and agent tools |
| `AGENT_LLM_PROVIDER` | Yes | — | Must be `openai`. Selects which LLM client `build_llm()` constructs |
| `AGENT_LLM_MODEL` | Yes | — | Model name passed to the provider. `gpt-4o-mini` is the tested model |
| `OPENAI_API_KEY` | Yes (openai) | — | Standard OpenAI API key. Charged per token at $0.15/1M input, $0.60/1M output |
| `LANGFUSE_PUBLIC_KEY` | No | — | Langfuse project public key. Tracing is disabled when absent |
| `LANGFUSE_SECRET_KEY` | No | — | Langfuse project secret key |
| `LANGFUSE_HOST` | No | — | Langfuse server URL. Use `http://localhost:3001` for the Docker instance |

**Loading `.env` when running agents:**

```bash
cd agents
env $(cat ../.env | grep -v '^#' | xargs) venv/bin/python main.py
```

Or export each variable manually:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export AGENT_LLM_PROVIDER=openai
export AGENT_LLM_MODEL=gpt-4o-mini
export OPENAI_API_KEY=sk-...
```

---

## Start Infrastructure (Docker)

```bash
cd docker
docker compose up -d
```

This starts 12 containers. Allow 60–90 seconds for all services to be ready.

**Verify all containers are running:**

```bash
docker compose ps
```

All services should show `Up` or `healthy`. Any `Exit` status means that container failed — check logs:

```bash
docker logs <container-name> --tail 50
```

**Service endpoints:**

| Service | URL | Credentials |
|---|---|---|
| Kafka broker | `localhost:9092` | — |
| Schema Registry | `http://localhost:8081` | — |
| Kafka Connect | `http://localhost:8083` | — |
| Flink Web UI | `http://localhost:8080` | — |
| Redpanda Console | `http://localhost:8088` | — |
| Prometheus | `http://localhost:9090` | — |
| Grafana | `http://localhost:3000` | admin / admin |
| Langfuse | `http://localhost:3001` | (create account on first visit) |

**Verify Kafka is reachable:**

```bash
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

Expect a list of API versions with no errors.

**Verify Flink UI is available:**

```bash
curl -s http://localhost:8080/jobs | python3 -m json.tool
```

Expect `{"jobs": []}` (empty list before jobs are submitted).

---

## Submit Flink SQL Jobs

Flink processes raw Kafka events and writes computed profiles to the profile topics. This must run before agents can do anything useful.

```bash
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/jobs.sql
```

The SQL client will print each statement and exit. The two INSERT jobs then run continuously as Flink streaming jobs.

**Verify jobs are running:**

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

Expected output (job IDs differ each run):

```
Running/Restarting Jobs
-----------------------
12:34:56    <id>    RUNNING    insert-into_default_catalog.default_database.live_user_profile
12:34:56    <id>    RUNNING    insert-into_default_catalog.default_database.live_product_profile
```

**Via the Web UI:**

Open `http://localhost:8080` → Running Jobs. Both jobs should show green with non-zero input records after producers start.

**Flink cold start behavior:**

When Flink first starts, `scan.startup.mode=latest-offset` means it only sees events that arrive *after* the job is submitted. If you restart Flink after producers have been running, it will miss historical events. The profile topics are log-compacted — the latest profile per user/product is retained — but Flink's aggregations restart from zero.

- After restart, `price_sensitivity='unknown'` until the first cart event arrives for each user.
- The agent trigger condition `price_sensitivity != 'unknown'` prevents recommendations until the profile is valid.
- Allow 2–5 minutes after Flink restart + producers running for valid profiles to appear.

---

## Start Data Producers

Run each producer in a separate terminal. They generate continuous simulated events.

**Terminal 1 — Clickstream (search, product_view, add_to_cart):**

```bash
source agents/venv/bin/activate
python -u producers/clickstream_producer.py
```

**Terminal 2 — Cart (purchase, return):**

```bash
source agents/venv/bin/activate
python -u producers/cart_producer.py
```

**Terminal 3 — Inventory (stock changes, price updates, sale toggles):**

```bash
source agents/venv/bin/activate
python -u producers/inventory_producer.py
```

**Terminal 4 — Product metadata (ratings, reviews, descriptions):**

```bash
source agents/venv/bin/activate
python -u producers/product_metadata_producer.py
```

**Verify producers are writing to Kafka:**

```bash
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic shoe-clickstream
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic cart-updates
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic inventory
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic product-metadata
```

Each topic should show increasing offsets. If any shows `0:0`, that producer is not running.

**Check the most recent message on a topic:**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic shoe-clickstream \
  --from-beginning \
  --max-messages 1
```

---

## Build Vector Index

The vector index is built automatically when the personalization agent first runs — `find_similar_products` calls `build_product_index()` which reads `product-metadata` from Kafka and indexes all 30 products into ChromaDB. No manual step is needed.

However, you can verify the index will have data by checking that the metadata topic has records:

```bash
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic product-metadata
```

If the offset is 0, run the metadata producer for at least 10 seconds before starting the agent.

**ChromaDB storage:**

ChromaDB persists to `./chroma_data/` relative to wherever the agent is run from. On restart, if the directory exists and the collection has data, the index is reused without rebuilding.

To force a full rebuild, delete the directory:

```bash
rm -rf agents/chroma_data
```

---

## Start Metrics Exporter

The metrics exporter reads Kafka profile topics and exports business metrics for Prometheus to scrape.

```bash
source agents/venv/bin/activate
python monitoring/kafka_exporter.py
```

**Verify metrics are available:**

```bash
curl -s http://localhost:8888/metrics | grep '^shoe_'
```

Expected output:

```
shoe_active_users_total 12
shoe_total_recommendations 47
shoe_recommendation_latency_seconds_average 2.31
shoe_recommendations_by_category{category="running"} 18
shoe_recommendations_by_category{category="lifestyle"} 22
shoe_top_recommended_product{product_id="NK-007"} 9
...
```

---

## Run Agent Loop

The agent loop watches `live-user-profile` and triggers personalization + merchandising for each new user profile. It uses OpenAI's API — each run costs roughly $0.001–$0.003 depending on tool call volume.

**Run in foreground (for development):**

```bash
cd agents
env $(cat ../.env | grep -v '^#' | xargs) venv/bin/python main.py
```

**Run in background (for extended monitoring):**

```bash
cd agents
nohup env $(cat ../.env | grep -v '^#' | xargs) venv/bin/python main.py > ../agent.log 2>&1 &
tail -f ../agent.log
```

**Expected output per agent run:**

```
[main] Processing user 42 (price_sensitivity=high)
[crew] Starting personalization crew for user 42
> Calling tool: Get Live User Profile
> Calling tool: Find Similar Products
> Calling tool: Get Price Qualified Products
[personalization] Product: Nike Free Run 5.0 (NK-007) | Price: $79.99 | ...
[merchandising] Top 3 products: ...
[kafka] Recommendation written to topic
[main] Run complete in 4.2s
```

**Agent trigger condition:**

The agent fires for a user when:
1. `price_sensitivity != 'unknown'` — Flink has computed at least one cart-based profile update
2. The user has not already been processed in the current session (tracked in `processed_users` set)

Users with `unknown` price sensitivity are skipped (Flink cold start state — not enough cart events yet).

**Merchandising cadence:**

The merchandising agent runs every N personalization cycles (configurable in `main.py`, default every user). It reads all product profiles and ranks the top 3 by promotion urgency: `(stock_trend == 'low', on_sale, demand_score)`.

---

## Observability Surfaces

### Langfuse — Agent Traces

Open `http://localhost:3001`.

On first visit, create a local account, then create an organization and project. Generate API keys from the project settings and put them in `.env`.

Once the agent runs with valid Langfuse keys, each crew run creates a trace:

- **Trace** — one full personalization or merchandising run
- **Spans** — one per tool call: `kafka.get_user_profile`, `vector.find_similar_products`, `kafka.get_price_qualified_products`, `kafka.write_recommendation`
- Each span includes input arguments, output (JSON), and duration

Traces appear within seconds of each agent run. Filter by trace name `personalization` or `merchandising`.

### Grafana — Business Metrics

Open `http://localhost:3000` (admin/admin).

Navigate to Dashboards → Shoe Personalization. The dashboard is provisioned automatically from `docker/grafana/dashboards/shoe-personalization.json`.

**Panels:**

| Panel | What it shows |
|---|---|
| Active users | Count of users with profiles in `live-user-profile` |
| Total recommendations | Cumulative count written to the `recommendations` topic |
| Recommendations by category | Breakdown by running / lifestyle / racing / training / hiking / football |
| Top recommended products | Product IDs ranked by recommendation count |
| Average recommendation latency | Mean time from profile read to recommendation write |

If all panels show "No data", check that the metrics exporter is running and that Prometheus is scraping it (`http://localhost:9090/targets`).

### Flink Web UI

Open `http://localhost:8080`.

- **Running Jobs** — Both INSERT jobs should show `RUNNING` with continuously growing record counts
- **Job graph** — Click a job to see the operator DAG (source → window → join → sink)
- **Task managers** — Shows memory and CPU utilization

### Redpanda Console — Kafka Visibility

Open `http://localhost:8088`.

- **Topics** — Browse all 7 topics, see message counts, preview messages
- **Consumer groups** — See the `agent-consumer-group` lag; a high lag means the agent is behind
- **Schema Registry** — View registered Avro/JSON schemas (not used here — all topics are JSON without schema registry enforcement)

---

## Smoke Test

The smoke test validates that all layers are up and producing valid output.

```bash
cd agents
env $(cat ../.env | grep -v '^#' | xargs) venv/bin/python smoke_test.py
```

**What it checks:**

1. Kafka connectivity — can produce and consume a test message
2. `live-user-profile` topic — at least one valid profile exists
3. `live-product-profile` topic — at least one valid product profile exists
4. Vector index — `find_similar_products` returns results
5. `Get Price Qualified Products` — returns a non-empty list for each sensitivity tier
6. Recommendation topic — at least one recommendation exists (if agents have run)

**Interpreting failures:**

| Failure | Likely cause |
|---|---|
| Kafka connection refused | Docker not running, or Kafka container crashed |
| No user profiles | Flink not running, or producers not started |
| No product profiles | Inventory/metadata producer not running, or Flink stopped |
| Vector index empty | `product-metadata` topic has no messages |
| No qualified products | Flink cold start — price_sensitivity all 'unknown', no valid profiles yet |

---

## Health Checks by Layer

### Layer 1: Kafka

**All topics have records:**

```bash
for topic in shoe-clickstream cart-updates inventory product-metadata live-user-profile live-product-profile recommendations; do
  echo -n "$topic: "
  docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic $topic 2>/dev/null | head -1
done
```

**Consumer group lag:**

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group agent-consumer-group \
  --describe
```

`LAG` column should be small (< 10). A large lag means the agent loop is processing slower than Flink writes.

**Read latest message from profile topic:**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic live-user-profile \
  --from-beginning \
  --max-messages 5
```

Each message should be a JSON object with fields: `userid`, `active_interest_category`, `price_sensitivity`, `avg_order_price`, `recent_searches`, `recent_cart_adds`, `total_orders`.

### Layer 2: Flink

**Jobs are running:**

```bash
curl -s http://localhost:8080/jobs | python3 -m json.tool
```

Both jobs should show `"status": "RUNNING"`.

**Jobs are processing records:**

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

**Job logs (last 50 lines):**

```bash
docker logs flink-jobmanager --tail 50
docker logs flink-taskmanager --tail 50
```

Look for `Exception` or `FAILED` in the logs. A common issue is task manager OOM — increase Docker memory allocation if you see GC overhead or heap errors.

**Restart Flink jobs after failure:**

```bash
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/jobs.sql
```

### Layer 3: Vector Search

**Index has documents:**

```python
# From a Python shell in the agents/ directory
import chromadb
client = chromadb.PersistentClient(path="./chroma_data")
col = client.get_collection("shoe_products")
print(col.count())  # Should be 30
```

**Test a query:**

```python
results = col.query(query_texts=["budget everyday running shoe"], n_results=5)
print(results["ids"])
print(results["distances"])
```

Distances should be in the range 0.3–0.7 (cosine distance for normalized embeddings). Values near 0 mean identical; values near 1 mean unrelated.

### Layer 4: Agents

**Test a single agent run manually:**

```bash
cd agents
env $(cat ../.env | grep -v '^#' | xargs) python3 -c "
from config.agents import personalization_agent
from tasks.tasks import create_personalization_task
from crewai import Crew, Process

task = create_personalization_task(userid=1)
crew = Crew(agents=[personalization_agent], tasks=[task], process=Process.sequential, verbose=True)
result = crew.kickoff()
print(result)
"
```

**Check that the recommendations topic is receiving output:**

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic recommendations \
  --from-beginning \
  --max-messages 5
```

### Layer 5: Observability

**Prometheus is scraping metrics:**

```bash
curl -s 'http://localhost:9090/api/v1/query?query=shoe_active_users_total' | python3 -m json.tool
```

Expect a non-empty `result` array.

**Grafana can reach Prometheus:**

Open `http://localhost:3000` → Connections → Data sources → Prometheus → Test. Should return "Data source is working."

**Langfuse is reachable:**

```bash
curl -s http://localhost:3001/api/public/health
```

Expect `{"status":"ok"}`.

---

## Debugging by Layer

### Producers not writing to Kafka

1. Confirm Kafka is up: `docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092`
2. Check the producer process is still running: `ps aux | grep producer`
3. Check producer stderr for connection errors
4. Confirm the topic exists:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Topics are auto-created on first produce if `auto.create.topics.enable=true` (which it is in this setup).

### Flink not writing to profile topics

1. Confirm both Flink jobs are RUNNING: `curl -s http://localhost:8080/jobs`
2. If no jobs, resubmit: `docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/jobs.sql`
3. Check taskmanager logs for exceptions: `docker logs flink-taskmanager --tail 100`
4. Verify source topics have data (Flink can't compute if nothing is arriving)
5. Check watermark progress in the Flink UI — if watermarks are stuck, event-time windows won't close

**Common Flink failure: task manager OOM**

Symptom: jobs disappear from the running list, taskmanager logs show `OutOfMemoryError`.

Fix:
1. Increase Docker Desktop memory to 8 GB (Settings → Resources)
2. Restart containers: `docker compose restart`
3. Resubmit jobs

### Agent reads stale or no profiles

**Symptom:** Agent loop logs show `Skipping user X: price_sensitivity=unknown`

This is the Flink cold start condition. Wait for cart events to flow through:

1. Confirm `cart_producer.py` is running
2. Check `cart-updates` topic has records
3. Wait 2–5 minutes for Flink's HOP window (15-minute size, 1-minute slide) to emit results
4. Watch `live-user-profile` for new messages:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic live-user-profile \
  --max-messages 3
```

**Symptom:** Agent reads profile but tools find no data

The progressive Kafka lookback may be exhausted. The `get_user_profile` tool scans up to 5M messages backward. If the profile topic has very few messages, the user profile may not be in the window. Check the topic offset:

```bash
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic live-user-profile
```

A low offset (< 100) early in the session is normal. The agent loop polls the consumer, so the profile will be found on the next poll cycle.

### Vector search returns no results or wrong results

**No results:**

The index is empty. Run the metadata producer for at least 30 seconds, then restart the agent (which rebuilds the index on first tool call).

**Wrong results (all results from one product):**

Check that the collection is using cosine distance. The collection is created with `metadata={"hnsw:space": "cosine"}` in `vector_tools.py`. If the `chroma_data/` directory has a stale collection created with L2 distance:

```bash
rm -rf agents/chroma_data
```

The index will rebuild on next agent run.

**Results not varying across users:**

The query must vary per user. The task prompt specifies that the query uses `avg_order_price` + `recent_searches` + `recent_cart_adds`, not just `category + sensitivity`. If queries are identical, check the LLM output in verbose mode — the agent may be ignoring the query construction instructions.

### LLM errors

**`AuthenticationError`:** `OPENAI_API_KEY` is missing or invalid. Check `.env` and re-run with `env $(cat ../.env | grep -v '^#' | xargs)`.

**`RateLimitError`:** OpenAI rate limit hit. Wait 60 seconds and retry. For development, the agent loop processes one user at a time, which keeps request rates low.

**`InternalServerError` (5xx):** OpenAI service issue. Retry usually resolves it.

**Agent gives wrong product (invented product ID):**

The task prompt instructs: "ONLY use productids that appeared in the tool results. Never invent a product." If you see a productid that doesn't match any tool output, the model hallucinated. This is rare with `temperature=0.2`. Check that the tool outputs are being correctly formatted (no JSON parse errors in tool functions).

### Langfuse traces not appearing

1. Confirm `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, and `LANGFUSE_HOST` are set
2. Test the Langfuse connection: `curl -s http://localhost:3001/api/public/health`
3. Check agent logs for Langfuse client errors
4. Traces are flushed asynchronously — wait 5–10 seconds after a run before checking

If Langfuse keys are missing, `observability.py` skips all tracing silently. The agent runs normally.

### Grafana shows "No data"

1. Check metrics exporter is running: `curl -s http://localhost:8888/metrics | grep '^shoe_'`
2. Check Prometheus is scraping: `http://localhost:9090/targets` — the `kafka-exporter` target should show `UP`
3. Check Prometheus has data: `curl -s 'http://localhost:9090/api/v1/query?query=shoe_active_users_total'`
4. In Grafana, verify the data source points to `http://prometheus:9090` (Docker internal network name, not `localhost`)

---

## Known Issues and Workarounds

### Flink OOM on Mac with 8GB Docker allocation

**Symptom:** Flink jobs disappear after ~30 minutes of running.

**Cause:** Flink's managed memory allocation exceeds Docker's configured limit.

**Workaround:** In `docker/docker-compose.yml`, reduce Flink memory flags:

```yaml
FLINK_PROPERTIES: |
  jobmanager.memory.process.size: 1024m
  taskmanager.memory.process.size: 1536m
```

Or reduce the number of parallel Flink tasks by running only the producers you need.

### Log compaction lag on profile topics

**Symptom:** The profile topics contain many messages for the same userid, not just the latest.

**Cause:** Kafka's log compaction runs on a background thread and does not compact immediately. Recent segments are not compacted until they are closed (i.e., a new segment starts).

**Effect on agents:** The Kafka consumer sees multiple messages per userid in one poll. The `get_user_profile` tool handles this correctly — it scans backward from the latest offset and takes the first (most recent) message for the requested userid.

**This is expected behavior.** Compaction will catch up over time.

### Agent processes user before first cart event

**Symptom:** Agent runs but produces no recommendation, or logs "price_sensitivity=unknown, skipping."

**Cause:** The agent trigger fires as soon as any message for a userid appears in `live-user-profile`. The first message for a new user may have been emitted by the pageview aggregation (no cart data yet), so `price_sensitivity` is `unknown`.

**Workaround:** The trigger condition `price_sensitivity != 'unknown'` is already in `main.py`. If you see users being skipped repeatedly, wait for `cart_producer.py` to generate cart events for those users.

### Consumer group offset reset after restart

**Symptom:** After restarting the agent loop, it re-processes users it already handled.

**Cause:** `processed_users` is an in-memory set, reset on each restart. The Kafka consumer group offset is stored in Kafka, so the consumer resumes from where it left off — but the in-memory deduplication set is empty.

**Effect:** Users may get a new recommendation on each agent restart. This is acceptable for a dev/demo system. For production, persist `processed_users` to Redis or a file.

### ChromaDB ONNX warning on first run

**Symptom:** `WARNING:chromadb.segment.impl.vector.local_hnsw: Number of requested results 10 is greater than number of elements in index 0`

**Cause:** The index is empty (first run before metadata producer has populated the topic).

**Fix:** Ensure `product_metadata_producer.py` has been running for at least 10 seconds before starting the agent. The index builds on first tool call.

### OpenAI tool call format failure

**Symptom:** Agent calls a tool with wrong argument types (e.g., passes `avg_order_price` as a string).

**Cause:** `gpt-4o-mini` occasionally wraps numeric values in quotes. CrewAI's tool execution layer handles type coercion in most cases, but edge cases exist.

**Fix:** The tool functions use explicit `float()` and `str()` casts on all inputs. This is already implemented.

---

## Teardown

**Stop the agent loop:**

```bash
# If running in foreground: Ctrl+C
# If running in background:
kill $(pgrep -f "main.py")
```

**Stop producers:**

```bash
kill $(pgrep -f "clickstream_producer")
kill $(pgrep -f "cart_producer")
kill $(pgrep -f "inventory_producer")
kill $(pgrep -f "product_metadata_producer")
```

**Stop Docker containers (keep volumes — data persists):**

```bash
cd docker
docker compose down
```

**Stop Docker containers and delete all data (full reset):**

```bash
cd docker
docker compose down -v
```

This deletes Kafka topic data, Flink checkpoints, ChromaDB files, Prometheus metrics, and Langfuse traces.

**Delete ChromaDB index only:**

```bash
rm -rf agents/chroma_data
```

The index rebuilds on the next agent run.

**Full clean state for a fresh demo:**

```bash
cd docker && docker compose down -v && cd ..
rm -rf agents/chroma_data
docker compose -f docker/docker-compose.yml up -d
# Wait 60s, then follow startup sequence from beginning
```
