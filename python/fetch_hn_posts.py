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
df_hn_posts["post_time"] = pd.to_datetime(
    df_hn_posts["time_unix"], unit="s", utc=True, errors="coerce"
)

print("HN posts shape:", df_hn_posts.shape)
print("Null url:", df_hn_posts["url"].isna().sum())
print("Type counts:\n", df_hn_posts["type"].value_counts(dropna=False).head(10))

df_hn_posts.head(10)


# %%
# =====================================
# 3) Filter GitHub URLs -> df_hn_github_posts
# =====================================

df_hn_posts["url"] = df_hn_posts["url"].fillna("").astype(str)

github_mask = df_hn_posts["url"].str.contains(r"github\.com", case=False, na=False)
df_hn_github_posts = df_hn_posts[github_mask].copy()

print("GitHub URLs found:", df_hn_github_posts.shape[0])
df_hn_github_posts[["hn_id", "url"]].head(10)


# %%
domains = df_hn_github_posts["url"].str.extract(r"https?://([^/]+)/")
print(domains.value_counts().head(10))


# %%
gist_mask = df_hn_github_posts["url"].str.contains(
    r"gist\.github\.com", case=False, na=False
)
print("Gist URLs:", gist_mask.sum())

df_hn_github_posts = df_hn_github_posts[~gist_mask].copy()
print("GitHub non-gist URLs:", df_hn_github_posts.shape[0])


# %%
# =====================================
# 4) Extract owner/repo/full_name from URL (robust)
# =====================================
pattern = r"github\.com/([^/]+)/([^/#?]+)"

df_hn_github_posts[["owner", "repo_raw"]] = df_hn_github_posts["url"].str.extract(
    pattern
)


df_hn_github_posts["repo"] = (
    df_hn_github_posts["repo_raw"]
    .str.replace(r"\.git$", "", regex=True)
    .str.replace(r"\s+", "", regex=True)
)


df_hn_github_posts["full_name"] = (
    df_hn_github_posts["owner"] + "/" + df_hn_github_posts["repo"]
)
df_hn_github_posts.head(10)


# %%
# Bad Extracts
bad = df_hn_github_posts[
    df_hn_github_posts["owner"].isna() | df_hn_github_posts["repo"].isna()
]
print("Bad extracts:", len(bad))
bad[["hn_id", "url"]].head(10)


# # Keep only rows with valid owner and repo
df_hn_github_posts = df_hn_github_posts.dropna(subset=["owner", "repo"]).copy()

print("Final GitHub repo links:", df_hn_github_posts.shape[0])
df_hn_github_posts[["hn_id", "full_name", "url"]].head(10)


# %%
