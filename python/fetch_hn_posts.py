# %%
import requests
from datetime import datetime, timezone
import pandas as pd
import os
from dotenv import load_dotenv
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np

# Load environment variables from .env
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
# =====================================
# 5) Fetch GitHub Repository Data -> df_github_repos
# =====================================

unique_repos = df_hn_github_posts[["owner", "repo", "full_name"]].drop_duplicates()
print("Unique repos:", unique_repos.shape[0])


headers = {
    "User-Agent": "hn_github_tracker/1.0 (data project)",
}


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if GITHUB_TOKEN:
    headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"


session = requests.Session()
session.headers.update(headers)


rows = []
success = 0
failed = 0


for r in unique_repos.itertuples():
    owner = r.owner
    repo = r.repo
    url = f"https://api.github.com/repos/{owner}/{repo}"

    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        rows.append(
            {
                "full_name": data.get("full_name"),
                "stars": data.get("stargazers_count"),
                "language": data.get("language"),
                "forks": data.get("forks_count"),
                "open_issues": data.get("open_issues_count"),
                "updated_at": data.get("updated_at"),
            }
        )
        success += 1

    except requests.exceptions.RequestException:
        failed += 1
        continue

df_github_repos = pd.DataFrame(rows)

print("Fetched OK:", success)
print("Failed:", failed)
print("df_github_repos shape:", df_github_repos.shape)

df_github_repos.head()


# %%
# =====================================
# 6) Prepare GitHub Data for MySQL
# =====================================

df_github_repos[["owner", "repo"]] = df_github_repos["full_name"].str.split(
    "/", expand=True
)

df_github_repos["updated_at"] = pd.to_datetime(
    df_github_repos["updated_at"], utc=True, errors="coerce"
).dt.tz_convert(None)


df_to_db = df_github_repos[
    [
        "full_name",
        "owner",
        "repo",
        "stars",
        "language",
        "forks",
        "open_issues",
        "updated_at",
    ]
].copy()

print("df_to_db shape:", df_to_db.shape)


df_to_db.head()


# %%
# =====================================
# 7) Connect to MySQL
# =====================================
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)

cursor = conn.cursor()
print("Connected to database")


# %%
# =====================================
# 8) Prepare rows for insert
# =====================================
repo_rows = []

for row in df_to_db.itertuples(index=False, name=None):
    repo_rows.append(row)

print("repo_rows len:", len(repo_rows))


# %%
# =====================================
# 9) Upsert GitHub Repositories
# =====================================

repo_insert_sql = """
INSERT INTO github_repos
(full_name, owner, repo, stars, language, forks, open_issues, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    stars = VALUES(stars),
    language = VALUES(language),
    forks = VALUES(forks),
    open_issues = VALUES(open_issues),
    updated_at = VALUES(updated_at);
"""

cursor.executemany(repo_insert_sql, repo_rows)
conn.commit()

print("GitHub repos upsert completed")


# %%
# =====================================
# 10) Prepare HN Posts for MySQL -> hn_to_db
# =====================================

hn_to_db = df_hn_posts.copy()

# clean url
hn_to_db["url"] = hn_to_db["url"].fillna("").astype(str)

# clean important text columns
hn_to_db["title"] = hn_to_db["title"].astype("string")
hn_to_db["author"] = hn_to_db["author"].astype("string")

# remove broken rows
hn_to_db = hn_to_db.dropna(subset=["hn_id", "title", "author", "post_time"]).copy()

# extract full_name from GitHub URLs for later linking
pattern = r"github\.com/([^/]+)/([^/#?]+)"
hn_to_db[["owner_tmp", "repo_tmp"]] = hn_to_db["url"].str.extract(pattern)

hn_to_db["repo_tmp"] = (
    hn_to_db["repo_tmp"]
    .str.replace(r"\.git$", "", regex=True)
    .str.replace(r"\s+", "", regex=True)
)

hn_to_db["full_name"] = hn_to_db["owner_tmp"] + "/" + hn_to_db["repo_tmp"]

# repo_id will be filled later
hn_to_db["repo_id"] = None

# keep only columns needed for MySQL
hn_to_db = hn_to_db[
    [
        "hn_id",
        "title",
        "author",
        "comments",
        "score",
        "post_time",
        "url",
        "repo_id",
        "full_name",
    ]
].copy()

print("hn_to_db shape:", hn_to_db.shape)
print("full_name not null:", hn_to_db["full_name"].notna().sum())

hn_to_db.head()


# %%
# =====================================
# 11) Prepare HN rows for insert
# =====================================

post_rows = []

for row in hn_to_db.itertuples(index=False, name=None):
    post_rows.append(row)

print("post_rows len:", len(post_rows))


# %%
# =====================================
# 12) Upsert HN Posts
# =====================================

post_insert_sql = """
INSERT INTO hn_posts
(hn_id, title, author, comments, score, post_time, url, repo_id, full_name)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    title = VALUES(title),
    author = VALUES(author),
    comments = VALUES(comments),
    score = VALUES(score),
    post_time = VALUES(post_time),
    url = VALUES(url),
    repo_id = VALUES(repo_id),
    full_name = VALUES(full_name);
"""

cursor.executemany(post_insert_sql, post_rows)
conn.commit()

print("HN posts upsert completed")


# %%
# =====================================
# 13) Link repo_id using UPDATE JOIN
# =====================================

link_sql = """
UPDATE hn_posts h
JOIN github_repos g
ON h.full_name = g.full_name
SET h.repo_id = g.repo_id;
"""

cursor.execute(link_sql)
conn.commit()

print("repo_id linking completed")


# %%
# =====================================
# 14) Close DB connection
# =====================================

cursor.close()
conn.close()


print("Connection closed")


# %%
# =====================================
# 15) Load Analysis Dataset from MySQL
# =====================================

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)

print("Connected to database")


query = """
SELECT 
    h.hn_id,
    h.score AS hn_score,
    h.comments AS hn_comments,
    g.repo_id,
    g.stars AS github_stars,
    g.forks AS github_forks,
    g.language
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id
WHERE h.score IS NOT NULL
  AND g.stars IS NOT NULL
  AND h.score > 0
  AND g.stars > 0;
"""


df_analysis = pd.read_sql(query, conn)

conn.close()
print("Connection closed")


print("df_analysis shape:", df_analysis.shape)


df_analysis.head()


# %%
# =====================================
# 16) Dataset Structure
# =====================================

df_analysis.info()


# %%
# =====================================
# 17) Summary Statistics
# =====================================

df_analysis.describe()


# %%
# =====================================
# 18) Missing Values Check
# =====================================

df_analysis.isna().sum()


# %%
# =====================================
# EDA Part 1: Core Distribution Analysis
# =====================================


# %%
# =====================================
# 19) HN Score Distribution
# =====================================

plt.hist(df_analysis["hn_score"], bins=12, edgecolor="black")
plt.title("Distribution of Hacker News Scores")
plt.xlabel("HN Score")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 20) GitHub Stars Distribution
# =====================================

plt.hist(df_analysis["github_stars"], bins=12, edgecolor="black")
plt.title("Distribution of GitHub Stars")
plt.xlabel("GitHub Stars")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 21) Log-Transformed GitHub Stars Distribution
# =====================================

df_analysis["log_github_stars"] = np.log1p(df_analysis["github_stars"])

plt.hist(df_analysis["log_github_stars"], bins=12, edgecolor="black")
plt.title("Distribution of Log-Transformed GitHub Stars")
plt.xlabel("Log(GitHub Stars + 1)")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 22) Log-Transformed HN Score Distribution
# =====================================

df_analysis["log_hn_score"] = np.log1p(df_analysis["hn_score"])

plt.hist(df_analysis["log_hn_score"], bins=12, edgecolor="black")
plt.title("Distribution of Log-Transformed HN Score")
plt.xlabel("Log(HN Score + 1)")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# EDA Part 2: Core Cross-Platform Relationship Analysis
# =====================================


# %%
# =====================================
# 23) HN Score vs Raw GitHub Stars
# =====================================

plt.scatter(df_analysis["github_stars"], df_analysis["hn_score"])
plt.title("HN Score vs Raw GitHub Stars")
plt.xlabel("GitHub Stars")
plt.ylabel("HN Score")
plt.show()


# %%
# =====================================
# 24) HN Score vs Log GitHub Stars
# =====================================

plt.scatter(df_analysis["log_github_stars"], df_analysis["hn_score"])
plt.title("HN Score vs Log GitHub Stars")
plt.xlabel("Log(GitHub Stars + 1)")
plt.ylabel("HN Score")
plt.show()


# %%
# =====================================
# 25) Correlation Comparison for HN Score
# =====================================

score_corr_summary = df_analysis[
    ["hn_score", "log_hn_score", "github_stars", "log_github_stars"]
].corr()

score_corr_summary


# %%
# =====================================
# EDA Part 3: Supporting Analysis
# =====================================


# %%
# =====================================
# 26) HN Comments Distribution
# =====================================

plt.hist(df_analysis["hn_comments"], bins=12, edgecolor="black")
plt.title("Distribution of Hacker News Comments")
plt.xlabel("HN Comments")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 27) Log-Transformed HN Comments Distribution
# =====================================

df_analysis["log_hn_comments"] = np.log1p(df_analysis["hn_comments"])

plt.hist(df_analysis["log_hn_comments"], bins=12, edgecolor="black")
plt.title("Distribution of Log-Transformed HN Comments")
plt.xlabel("Log(HN Comments + 1)")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 28) HN Comments vs Log GitHub Stars
# =====================================

plt.scatter(df_analysis["log_github_stars"], df_analysis["hn_comments"])
plt.title("HN Comments vs Log GitHub Stars")
plt.xlabel("Log(GitHub Stars + 1)")
plt.ylabel("HN Comments")
plt.show()


# %%
# =====================================
# 29) Correlation Comparison for HN Comments
# =====================================

comments_corr_summary = df_analysis[
    ["hn_comments", "log_hn_comments", "github_stars", "log_github_stars"]
].corr()

comments_corr_summary


# %%
# =====================================
# 30) Final Correlation Summary
# =====================================

final_corr_summary = pd.DataFrame(
    {
        "relationship": [
            "hn_score vs github_stars",
            "hn_score vs log_github_stars",
            "log_hn_score vs log_github_stars",
            "hn_comments vs github_stars",
            "hn_comments vs log_github_stars",
            "log_hn_comments vs log_github_stars",
        ],
        "correlation": [
            df_analysis["hn_score"].corr(df_analysis["github_stars"]),
            df_analysis["hn_score"].corr(df_analysis["log_github_stars"]),
            df_analysis["log_hn_score"].corr(df_analysis["log_github_stars"]),
            df_analysis["hn_comments"].corr(df_analysis["github_stars"]),
            df_analysis["hn_comments"].corr(df_analysis["log_github_stars"]),
            df_analysis["log_hn_comments"].corr(df_analysis["log_github_stars"]),
        ],
    }
)


# round for readability
final_corr_summary["correlation"] = final_corr_summary["correlation"].round(3)

# add trend column
final_corr_summary["trend"] = final_corr_summary["correlation"].apply(
    lambda x: "positive" if x > 0 else "negative" if x < 0 else "no relationship"
)

final_corr_summary


# %%
# =====================================
# EDA Part 4: Forks Analysis
# =====================================


# %%
# =====================================
# 31) GitHub Forks Distribution
# =====================================

plt.hist(df_analysis["github_forks"], bins=12, edgecolor="black")
plt.title("Distribution of GitHub Forks")
plt.xlabel("GitHub Forks")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 32) Log-Transformed GitHub Forks Distribution
# =====================================

df_analysis["log_github_forks"] = np.log1p(df_analysis["github_forks"])

plt.hist(df_analysis["log_github_forks"], bins=12, edgecolor="black")
plt.title("Distribution of Log-Transformed GitHub Forks")
plt.xlabel("Log(GitHub Forks + 1)")
plt.ylabel("Frequency")
plt.show()


# %%
# =====================================
# 33) HN Score vs Raw GitHub Forks
# =====================================

plt.scatter(df_analysis["github_forks"], df_analysis["hn_score"])
plt.title("HN Score vs Raw GitHub Forks")
plt.xlabel("GitHub Forks")
plt.ylabel("HN Score")
plt.show()


# %%
# =====================================
# 34) HN Score vs Log GitHub Forks
# =====================================

plt.scatter(df_analysis["log_github_forks"], df_analysis["hn_score"])
plt.title("HN Score vs Log GitHub Forks")
plt.xlabel("Log(GitHub Forks + 1)")
plt.ylabel("HN Score")
plt.show()


# %%
# =====================================
# 35) Correlation Comparison for HN Score vs Forks
# =====================================

forks_corr_summary = df_analysis[
    ["hn_score", "log_hn_score", "github_forks", "log_github_forks"]
].corr()

forks_corr_summary
