from src.load.open_aq import fetch_openaq_data, load_to_bronze
from src.utils.creds_gcp import create_bq_client
import os

def run_pipeline():
    # Initialisation
    bq_client = create_bq_client()
    api_key = os.getenv("OPEN_QUALITY_KEY_API")
    
    data = fetch_openaq_data(api_key, "2025-01-01")
    
    if data:
        load_to_bronze(bq_client, data)

if __name__ == "__main__":
    run_pipeline()