# Operations Runbook

This runbook is for proving the project works locally and debugging it when one layer is quiet.

## Prerequisites

You need Docker Desktop running, Python 3.11, and Ollama if you want to run the CrewAI agent layer with the default local model.

Install Python dependencies in a virtual environment:

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The existing repo also contains `agents/venv`, but a fresh `.venv` is easier to recreate and is ignored by git.

## Start Infrastructure

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization/docker
docker compose up -d
```

Expected local endpoints:

| Service | URL |
| --- | --- |
| Kafka | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |
| Kafka Connect | `http://localhost:8083` |
| Flink UI | `http://localhost:8080` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |

## Submit Flink Jobs

```bash
docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/jobs.sql
```

Check that jobs exist:

```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

You should see insert jobs for `live_user_profile` and `live_product_profile`.

## Start Data Producers

Run these in separate terminals:

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
source .venv/bin/activate
python -u producers/clickstream_producer.py
```

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
source .venv/bin/activate
python -u producers/cart_producer.py
```

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
source .venv/bin/activate
python -u producers/inventory_producer.py
```

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
source .venv/bin/activate
python -u producers/product_metadata_producer.py
```

## Start Metrics Exporter

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization
source .venv/bin/activate
python monitoring/kafka_exporter.py
```

Then check:

```bash
curl -s http://localhost:8888/metrics | grep '^shoe_'
```

## Run Agents

Install and start the default local model:

```bash
ollama pull qwen3.5:4b
ollama serve
```

Run a one-shot interactive test:

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization/agents
../.venv/bin/python test.py
```

Run continuous mode:

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization/agents
../.venv/bin/python main.py
```

## Health Checks

Compile the project Python files without scanning virtualenvs:

```bash
find producers agents monitoring -path '*/venv*' -prune -o -name '*.py' -print0 | xargs -0 python3 -m py_compile
```

Validate Docker Compose syntax:

```bash
docker compose -f docker/docker-compose.yml config --quiet
```

Run the project smoke test after Docker, Flink, producers, exporter, and Ollama are running:

```bash
cd /Users/advaitdarbare/Desktop/shoe-personalization/agents
../.venv/bin/python smoke_test.py
```

## Debugging By Layer

If producers fail, check that Kafka is reachable:

```bash
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

If source topics are empty:

```bash
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic shoe-clickstream
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic cart-updates
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic inventory
docker exec kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic product-metadata
```

If live profile topics are empty, Flink is usually the layer to inspect:

```bash
docker logs flink-jobmanager --tail 100
docker exec flink-jobmanager /opt/flink/bin/flink list
```

If the vector search tool says the product index is empty, make sure `product_metadata_producer.py` has produced records.

If Grafana is empty, check the chain in this order: exporter metrics, Prometheus targets, Grafana data source.

```bash
curl -s http://localhost:8888/metrics | grep '^shoe_'
open http://localhost:9090/targets
open http://localhost:3000
```

## Environment Variables

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Agent Kafka tools |
| `AGENT_LLM_PROVIDER` | `ollama` | Agent LLM config |
| `AGENT_LLM_MODEL` | `qwen3.5:4b` | Agent LLM config |
| `AGENT_LLM_BASE_URL` | `http://localhost:11434` | Agent LLM config |

## Known Healthy Signal

A healthy run has messages in raw source topics, messages in both live profile topics, exporter metrics beginning with `shoe_`, and a Grafana dashboard with non-empty panels.
