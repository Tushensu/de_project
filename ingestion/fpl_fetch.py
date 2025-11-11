import requests
import json
from config import FPL_BASE_URL

def fetch_bootstrap():
    url = f"{FPL_BASE_URL}/bootstrap-static/"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def fetch_teams():
    url = f"{FPL_BASE_URL}/teams/"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def fetch_players():
    # Actually bootstrap contains “elements” which are players
    data = fetch_bootstrap()
    return data['elements']

# Additional endpoints can be added as needed (e.g., fixtures, event history)
