# Building a Small Real-Time AI Context Pipeline with Kafka, Flink, and CrewAI

I built this project because I wanted to understand how streaming data can become useful context for an AI agent.

I was not trying to build a huge production recommendation system. The goal was simpler: learn how Kafka, Flink, and CrewAI fit together in one small end-to-end project.

The idea came from a real-time personalization architecture diagram. The diagram showed clickstream events, cart updates, and inventory changes flowing into Kafka. Flink transformed those events into live user and product context. Then agents used that context to make recommendations or trigger actions.

That pattern felt useful, but also a little abstract. So I turned it into a shoe store demo.

I also kept the project diagram in Excalidraw so it matches the hand-drawn feel of the original architecture sketch instead of turning into a heavy enterprise diagram.

## The Basic Idea

The project simulates a shoe retailer.

Users browse products, search for categories, add items to cart, and buy or return shoes. Inventory changes over time. Product metadata includes names, descriptions, attributes, ratings, and review counts.

All of those events flow through this simple pipeline:

```text
events -> Kafka -> Flink -> live context -> CrewAI agent -> recommendation
```

The important part is that the agent does not guess from stale data. It reads live context that was built from streaming events.

## What Kafka Does

Kafka is the event backbone.

In the project, there are four producer scripts:

- `clickstream_producer.py` sends searches, product views, and add-to-cart events
- `cart_producer.py` sends purchases and returns
- `inventory_producer.py` sends stock, price, and sale updates
- `product_metadata_producer.py` sends product descriptions, ratings, and attributes

Each producer writes JSON events into Kafka topics.

This was one of the first useful lessons for me: Kafka lets each part of the system stay separate. The clickstream producer does not need to know about Flink. Flink does not need to know about CrewAI. The agent does not need to know who produced the original events.

They all meet through Kafka.

## What Flink Does

Kafka stores the stream of events. Flink turns those events into current context.

In this project, Flink SQL builds two live profiles:

- `live-user-profile`
- `live-product-profile`

The user profile includes signals like recent searches, recent cart adds, active category, order count, average order price, and price sensitivity.

The product profile includes stock, sale status, demand score, rating, and category.

This is the part that made the project feel like more than a basic recommendation script. Instead of asking the AI model to figure everything out from raw events, Flink prepares structured context first.

## What CrewAI Does

CrewAI is the agent layer.

The personalization agent has tools that let it:

- read a user profile from Kafka
- read product profiles from Kafka
- search product metadata using ChromaDB
- filter products by price sensitivity
- write the final recommendation back to Kafka

The agent is not responsible for everything. Some decisions stay in normal Python code.

For example, price filtering is deterministic. If a user is highly price sensitive, the code filters out expensive shoes before the model sees the final candidate list. That keeps the demo easier to reason about and avoids asking the LLM to do simple arithmetic perfectly every time.

## What I Mean by an AI Context Platform

In this project, an AI context platform just means a system that keeps useful context fresh enough for an AI agent to use.

That sounds bigger than it is here. In this demo, the context platform is basically:

- Kafka topics holding raw events
- Flink jobs creating live user and product profiles
- a small vector index for product metadata
- CrewAI tools that read the live context
- an optional MCP server that exposes the same context through standard tools

The useful idea is that the agent is not working from memory, a static CSV file, or a nightly batch job. It is working from profiles that update as events arrive.

I kept MCP intentionally small. It does not replace Kafka or Flink. It just gives clients tools like `get_live_user_profile`, `get_live_product_catalog`, and `search_similar_products` so they do not need to know the internal topic names.

## The End-to-End Flow

Here is one example flow:

1. A user searches for running shoes and views a few running products.
2. The clickstream producer sends those events to Kafka.
3. The same user buys a mid-priced shoe.
4. The cart producer sends that event to Kafka.
5. Flink updates the user's live profile.
6. The profile now says the user's active category is `running` and their price sensitivity is `medium`.
7. The CrewAI agent reads that profile.
8. The agent searches for similar running shoes in ChromaDB.
9. Python code filters the candidates by price.
10. The agent writes one recommendation to the `recommendations` Kafka topic.

The final output is intentionally simple:

```text
Product: Nike Pegasus 40 (NK-002)
Price: $119.99
Live signal: running interest from recent browsing and cart activity
Why this user: medium price sensitivity matches their average order price
Stock: 37 units, medium trend
```

## What I Learned

The biggest thing I learned is that the AI part is only one piece of the system.

If the context is old or messy, the recommendation will probably be bad. Kafka and Flink matter because they make the context current and structured.

I also learned that it helps to keep some logic outside the model. The model is useful for combining context and explaining a recommendation. But filtering prices, reading Kafka, ranking by stock, and writing results back to Kafka are better handled in code.

Another lesson was that smaller demos are easier to learn from. It is tempting to add more services, dashboards, databases, APIs, and UI layers. For this version, I tried to keep the project focused on the core loop.

## What I Would Improve Next

If I keep building this, I would add a small UI that shows live user profiles and recommendations as they update.

I would also add schema validation for events, because right now the project relies on the producers and Flink SQL staying aligned.

Another useful improvement would be connecting the MCP server to a small UI or another agent client. Right now it is mainly a clean access layer around the live context tools.

But for now, the project does what I wanted it to do: it shows how Kafka, Flink, and CrewAI can work together in a small real-time personalization pipeline.

It is not a massive system. It is a learning project. And that is exactly the point.
