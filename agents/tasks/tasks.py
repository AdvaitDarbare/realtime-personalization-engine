from crewai import Task
from config.agents import personalization_agent, merchandising_agent


def create_personalization_task(userid: int) -> Task:
    return Task(
        description=f"""Analyze the live profile for user {userid} and recommend 
        the best shoe for them right now.
        
        Steps:
        1. Get user {userid}'s live profile using the Get Live User Profile tool
        2. Note their active interest category, searches, cart adds, price sensitivity,
           total orders and avg order price
        3. Use the Find Similar Products tool with a query based on the user's active
           interest category and behavior (e.g. "cushioned running shoe" if they browse running).
           This returns the most semantically relevant products for this user.
        4. For the top matches, check live stock and price using Get Live Product Profile
           or Get All Products.
        5. Recommend the single best product. Do not include alternatives.
        
        Price sensitivity guide:
        - high sensitivity: look for products under $80 or on sale
        - medium sensitivity: products $80-$120
        - low sensitivity: any product including premium ones
        
        Return only this final format:
        Product: <product name> (<productid>)
        Price: <price, and sale price if on sale>
        Live signal: <active category/search/cart signal that drove this>
        Why this user: <one sentence tied to price sensitivity and behavior>
        Stock: <stock count and stock_trend>

        Use the tool results you receive. Do not ask the user for tool output.
        If the profile and product list are available, produce the final recommendation.
        Be precise with prices: do not say a product is under $80 unless its actual or sale price is under $80.
        Do not include alternatives, backup choices, rankings, key findings, or extra sections.""",
        expected_output="""Exactly five lines:
        Product: <product name> (<productid>)
        Price: <price, and sale price if on sale>
        Live signal: <active category/search/cart signal that drove this>
        Why this user: <one sentence tied to price sensitivity and behavior>
        Stock: <stock count and stock_trend>""",
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
