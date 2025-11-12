import requests
import time
import logging
import os
from typing import Dict, Any

# --------------------------
# Retry / rate-limit helper
# --------------------------
def retry_request(url: str, retries: int = 3, delay: float = 1.0) -> Dict[str, Any]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            time.sleep(delay)
    logging.error(f"Failed after {retries} retries: {url}")
    return {}  # return empty dict on failure

# --------------------------
# FPL API fetchers
# --------------------------
BASE_URL = "https://fantasy.premierleague.com/api"

ENDPOINTS = [
    "bootstrap-static/",
    "elements/",
    "events/",
    "fixtures/"
]

def fetch_bootstrap() -> Dict:
    """Fetch general FPL data (bootstrap-static)"""
    url = f"{BASE_URL}/bootstrap-static/"
    return retry_request(url)

def fetch_all_endpoints() -> Dict[str, Dict]:
    """Fetch all main endpoints"""
    data = {}
    for ep in ENDPOINTS:
        key = ep.replace('/', '')
        url = f"{BASE_URL}/{ep}"
        data[key] = retry_request(url)
        time.sleep(0.5)  # rate limiting
    return data

def fetch_player_summaries(player_ids: list) -> Dict[int, Dict]:
    """Fetch element-summary for each player"""
    summaries = {}
    for pid in player_ids:
        url = f"{BASE_URL}/element-summary/{pid}/"
        summaries[pid] = retry_request(url)
        time.sleep(0.5)  # respect rate limit
    return summaries
