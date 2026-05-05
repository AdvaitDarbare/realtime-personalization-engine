import time
import json
from kafka import KafkaConsumer
from crew import run_personalization, run_merchandising

def watch_for_updates():
    """Continuously watch live-user-profile topic and trigger agents."""
    
    print("Starting Shoe Personalization System...")
    print("Watching for user profile updates...\n")
    
    consumer = KafkaConsumer(
        'live-user-profile',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',  # only new updates
        enable_auto_commit=True,
        group_id='agent-trigger-group',
        consumer_timeout_ms=60000
    )
    
    processed_users = set()
    last_merch_run = 0
    
    for message in consumer:
        profile = message.value
        if not profile:
            continue
            
        userid = profile.get('userid')
        total_orders = profile.get('total_orders', 0)
        
        # Only trigger agent if user has at least 2 orders
        # and we haven't processed them recently
        if total_orders >= 2 and userid not in processed_users:
            print(f"\nTriggering Personalization Agent for user {userid}")
            print(f"Profile: orders={total_orders}, sensitivity={profile.get('price_sensitivity')}")
            
            try:
                result = run_personalization(userid)
                print(f"\nRecommendation for user {userid}:")
                print(result)
                processed_users.add(userid)
            except Exception as e:
                print(f"Error running agent for user {userid}: {e}")
        
        # Run merchandising agent every 5 minutes
        current_time = time.time()
        if current_time - last_merch_run > 300:
            print("\nTriggering Merchandising Agent...")
            try:
                result = run_merchandising()
                print("\nMerchandising Recommendations:")
                print(result)
                last_merch_run = current_time
            except Exception as e:
                print(f"Error running merchandising agent: {e}")

if __name__ == "__main__":
    watch_for_updates()