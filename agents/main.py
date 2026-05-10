import time
import json
import os
from pathlib import Path
from kafka import KafkaConsumer


ROOT = Path(__file__).resolve().parents[1]


def load_env_file():
    env_path = ROOT / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from crew import run_personalization, run_merchandising

def watch_for_updates():
    """Continuously watch live-user-profile topic and trigger agents."""
    
    print("Starting Shoe Personalization System...")
    print("Watching for user profile updates...\n")
    
    consumer = KafkaConsumer(
        'live-user-profile',
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',  # only new updates
        enable_auto_commit=True,
        group_id='agent-trigger-group',
        consumer_timeout_ms=60000,
        max_poll_interval_ms=900000,
        max_poll_records=1,
    )
    
    processed_users = set()
    last_merch_run = 0
    
    for message in consumer:
        profile = message.value
        if not profile:
            continue
            
        userid = profile.get('userid')
        total_orders = profile.get('total_orders', 0)
        
        # Trigger when user has a valid price sensitivity (at least 1 cart event)
        # and we haven't processed them in this session
        price_sensitivity = profile.get('price_sensitivity', 'unknown')
        if price_sensitivity != 'unknown' and userid not in processed_users:
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
