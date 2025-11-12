import requests
import time
import json
from utils import retry_request, upload_to_s3
from config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY

BASE_URL = "https://fantasy.premierleague.com/api"

ENDPOINTS = [
    "bootstrap-static/",
    "elements/",
    "events/",
    "fixtures/"
]

def fetch_all_players_summary(player_ids):
    results = {}
    for pid in player_ids:
        url = f"{BASE_URL}/element-summary/{pid}/"
        data = retry_request(url)
        results[pid] = data
        time.sleep(0.5)  # respect rate limits
    return results

def fetch_all_endpoints():
    all_data = {}
    for ep in ENDPOINTS:
        url = f"{BASE_URL}/{ep}"
        all_data[ep.replace('/', '')] = retry_request(url)
        time.sleep(0.5)
    return all_data

def run_ingestion(s3_client):
    # fetch general endpoints
    raw_data = fetch_all_endpoints()
    for name, data in raw_data.items():
        upload_to_s3(f"{name}.json", data, s3_client)

    # fetch player-specific summaries
    player_ids = [p['id'] for p in raw_data['elements']['elements']]
    player_summaries = fetch_all_players_summary(player_ids)
    for pid, data in player_summaries.items():
        upload_to_s3(f"player_{pid}.json", data, s3_client)
