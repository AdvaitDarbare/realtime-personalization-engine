from crewai import Task
from config.agents import personalization_agent, merchandising_agent


def create_personalization_task(userid: int) -> Task:
    return Task(
        description=f"""Analyze the live profile for user {userid} and recommend
        the best shoe for them right now.

        Steps:
        1. Get user {userid}'s live profile using the Get Live User Profile tool.

        2. Note their active_interest_category, recent_searches, recent_cart_adds,
           price_sensitivity, total_orders, and avg_order_price.

        3. Call Find Similar Products with TWO arguments:
           - query: a rich 4-8 word description built from MULTIPLE user signals.
             Combine adjectives from: category + price tier + behavior signals below.
             Use avg_order_price for precise price signals, not just high/medium/low.
             Use recent_searches and recent_cart_adds for intent signals.

             Build the query differently for each user. Examples:
               running + avg_order_price $70 + many cart_adds  → "budget everyday running shoe active shopper"
               running + avg_order_price $100 + low sensitivity → "premium cushioned marathon daily trainer"
               running + avg_order_price $90  + few searches   → "comfortable neutral daily running shoe"
               lifestyle + avg_order_price $65 + many searches → "trendy street casual sneaker popular"
               lifestyle + avg_order_price $75 + many cart_adds → "versatile canvas classic retro shoe"
               lifestyle + avg_order_price $60 + few orders    → "entry-level casual everyday lifestyle sneaker"
               racing                                          → "carbon plate race day fast performance"
               training                                        → "stable cross training gym workout shoe"
             Vary the query based on avg_order_price and behavior signals.
             Avoid repeating the same query template for every user in the same category.

           - category: pass the user's active_interest_category exactly
             (e.g. "running", "lifestyle", "racing", "training", "football", "hiking").
             This pre-filters results to the right category before ranking.

        4. Call Get Price Qualified Products with THREE arguments:
           - price_sensitivity: the user's price_sensitivity value exactly
           - avg_order_price: the user's avg_order_price value exactly
           - category: the user's active_interest_category exactly
           This returns ONLY products that qualify for the user's price tier.
           Products outside the price range are already excluded — trust this result.
           You MUST call this tool. Do not skip it.

        5. Match: from step 3 (similarity results) and step 4 (qualified products),
           find products that appear in BOTH lists.
           - Pick the one with the highest similarity_score from step 3.
           - TIEBREAKER (within 0.05 similarity): pick higher demand_score from step 4.
           - If NO overlap: pick the product with highest demand_score from step 4.

        6. Recommend the single best product. Do not include alternatives.

        CRITICAL — YOUR ENTIRE RESPONSE MUST BE EXACTLY THESE 5 LINES. NO OTHER TEXT.
        NO markdown, NO headers, NO bullet points, NO explanations before or after.
        ONLY use productids that appeared in the tool results. Never invent a product.

        Product: <product name from tool results> (<productid from tool results>)
        Price: $<price> (sale $<sale_price> if on_sale is true)
        Live signal: <active_interest_category and recent behavior from profile>
        Why this user: <one sentence: price_sensitivity + behavior signal>
        Stock: <stock number> units, <stock_trend> trend""",
        expected_output="""Exactly five plain-text lines with no markdown:
        Product: <name> (<productid>)
        Price: $<amount>
        Live signal: <signal>
        Why this user: <one sentence>
        Stock: <n> units, <trend> trend""",
        agent=personalization_agent
    )


def create_merchandising_task() -> Task:
    return Task(
        description="""Analyze all live product profiles and identify the top 3 
        products that need promotion right now.
        
        Steps:
        1. Get all product profiles using Get All Products tool
        2. Look for products with stock_trend = "low" (urgent)
        3. Look for products where on_sale = true (needs visibility)
        4. Look for products with highest demand_score
        5. Rank top 3 by promotion urgency
        
        For each product provide:
        - Product name and ID
        - Why it needs promotion right now
        - Recommended channel: email, homepage_banner, or push_notification
        - Expected impact""",
        expected_output="""Top 3 products to promote with:
        - Product name and ID
        - Reason for promotion
        - Recommended channel
        - Expected impact""",
        agent=merchandising_agent
    )
