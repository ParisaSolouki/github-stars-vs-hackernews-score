# %%
import requests
from datetime import datetime, timezone
import pandas as pd
import os
from dotenv import load_dotenv
import mysql.connector

load_dotenv()


# %%
# =====================================
# 1) Fetch Hacker News Top Stories IDs
# =====================================
TOPSTORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty"


def fetch_topstories_ids(url=TOPSTORIES_URL, timeout=10):
    run_time_utc = datetime.now(timezone.utc)
    headers = {"User-Agent": "hn_github_tracker/1.0 (data project)"}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        ids = resp.json()

        # validation ids type
        if not isinstance(ids, list):
            raise ValueError("Unexpected response: topstories is not a list")

        print("Request successful")
        print("Run time (UTC):", run_time_utc.isoformat())
        print("Total IDs fetched:", len(ids))
        print("First 5 IDs:", ids[:5])

        return ids, run_time_utc

    except requests.exceptions.RequestException as e:
        print("Request failed", str(e))
        return [], run_time_utc

    except ValueError as e:
        print("data validation failed", str(e))
        return [], run_time_utc


top_ids, run_time_utc = fetch_topstories_ids()


# %%
# =====================================
# 2) Extract Posts Data -> df_hn_posts
# =====================================
session = requests.Session()
headers = {"User-Agent": "hn_github_tracker/1.0 (data project)"}
session.headers.update(headers)


rows = []


for item_id in top_ids:
    item_url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"

    try:
        resp = session.get(item_url, timeout=10)
        resp.raise_for_status()
        item = resp.json()

        if not isinstance(item, dict):
            continue

        rows.append(
            {
                "hn_id": item.get("id"),
                "title": item.get("title"),
                "author": item.get("by"),
                "comments": item.get("descendants"),
                "score": item.get("score"),
                "time_unix": item.get("time"),
                "url": item.get("url"),
                "type": item.get("type"),
            }
        )

    except requests.exceptions.RequestException:
        continue


df_hn_posts = pd.DataFrame(rows)
df_hn_posts.head(10)


# %%
