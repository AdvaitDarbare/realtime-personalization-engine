import os
from crewai import Agent, LLM


def build_llm() -> LLM:
    model = os.getenv("AGENT_LLM_MODEL", "gpt-4o-mini").strip()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is required. Add it to .env or export it before running the agents."
        )
    if ":" in model and not model.startswith("ft:"):
        raise RuntimeError(
            "AGENT_LLM_MODEL should be an OpenAI model name such as gpt-4o-mini."
        )
    return LLM(
        model=model,
        provider="openai",
        temperature=0.2,
        timeout=120,
    )


def build_personalization_agent(tools: list, llm: LLM) -> Agent:
    return Agent(
        role="Personalization Specialist",
        goal="Recommend the perfect shoe to each user based on their live behavior and preferences",
        backstory="""You are an expert in real-time personalization for a shoe retailer.
    You analyze live user behavior data: recent intent-window activity,
    orders, price sensitivity, and recommend the most relevant products.
    You always consider:
    - The user's price sensitivity (high/medium/low)
    - Their order history and total orders
    - Current stock availability of products
    - Whether products are on sale
    You give specific, actionable recommendations with clear reasoning.
    Always recommend exactly ONE product with a clear reason why.""",
        tools=tools,
        llm=llm,
        verbose=True,
    )


def build_merchandising_agent(tools: list, llm: LLM) -> Agent:
    return Agent(
        role="Merchandising Specialist",
        goal="Identify which products to promote based on live demand and inventory signals",
        backstory="""You are an expert merchandiser for a shoe retailer.
    You analyze live product data: stock levels, demand scores, sale status,
    and decide which products need promotion right now. You always look for:
    - Products with high demand but low stock (urgency signal)
    - Products on sale that need more visibility
    - Products with high demand scores
    You recommend exactly 3 products to promote with specific channels:
    email, homepage banner, or push notification.""",
        tools=tools,
        llm=llm,
        verbose=True,
    )
