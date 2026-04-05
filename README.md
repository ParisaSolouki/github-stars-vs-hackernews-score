# GitHub Stars vs Hacker News Score Analysis

This project explores whether GitHub-popular repositories also receive strong attention on Hacker News.

The main goal is to analyze the relationship between:

- **GitHub popularity** → measured by repository **stars**
- **Hacker News engagement** → measured by **score** and **comments**

---

## Project Question

**Do repositories with more GitHub stars also tend to receive more attention on Hacker News?**

---

## Dataset Overview

This project combines data from two public APIs:

- **Hacker News API**
- **GitHub API**

### Final analysis dataset includes:

- `hn_id`
- `title`
- `hn_score`
- `hn_comments`
- `repo_id`
- `full_name`
- `github_stars`
- `language`

Each row represents:

> **one Hacker News post linked to one GitHub repository**

---

## Tools & Technologies

- **Python**
  - pandas
  - NumPy
  - matplotlib
  - requests
  - mysql.connector
- **MySQL**
- **DBeaver**
- **VS Code**
- **Git / GitHub**

---

## Project Workflow

### 1) Data Collection
- Fetched top stories from the **Hacker News API**
- Filtered posts containing **GitHub repository links**
- Extracted repository owner/name from URLs
- Retrieved repository metadata from the **GitHub API**

### 2) Data Storage
- Stored cleaned data in **MySQL**
- Created two main tables:
  - `hn_posts`
  - `github_repos`

### 3) Analysis Dataset Creation
- Joined Hacker News post data with GitHub repository data
- Built a final dataset for analysis

### 4) Exploratory Data Analysis (EDA)
- Checked dataset structure, missing values, duplicates, and grain
- Analyzed distributions of:
  - HN score
  - HN comments
  - GitHub stars
- Applied **log transformations** to reduce skew
- Explored relationships using:
  - scatter plots
  - correlation analysis
  - quartile segmentation
  - performance gap ranking

---

## Key Findings

### 1) GitHub stars and Hacker News score show a positive relationship
Repositories with more GitHub stars generally tended to receive higher Hacker News scores.

### 2) Log-transforming GitHub stars made the relationship clearer
Raw star counts were highly skewed, and a log transformation revealed the relationship more clearly.

### 3) GitHub stars were also positively associated with HN comments
More popular repositories not only tended to get more visibility, but also more discussion.

### 4) Popularity alone did not fully explain Hacker News performance
Some repositories performed **better or worse than expected** relative to their GitHub popularity.

### 5) Overperformers and underperformers were identified
By comparing relative GitHub popularity and HN attention, the project highlighted repositories that:
- **overperformed** on Hacker News
- **underperformed** despite having high GitHub popularity

---

## Main Insight

> **GitHub popularity helps explain Hacker News performance, but it does not fully determine it.**

This suggests that additional factors such as:
- topic relevance
- timing
- presentation
- audience interest

may also influence Hacker News engagement.

---

## Example Analysis Techniques Used

- Data cleaning
- API data extraction
- SQL joins
- Exploratory Data Analysis (EDA)
- Correlation analysis
- Log transformation
- Quartile-based segmentation
- Relative performance comparison

---

## Project Structure

```bash
GITHUB-STARS-VS-HACKERNEWS-SCORE/
│
├── data/
│
├── python/
│   └── fetch_hn_posts.py
│
├── sql/
│   ├── schema.sql
│   └── analysis.sql
│
├── .env
├── .gitignore
└── README.md
```

### File Description

- **python/fetch_hn_posts.py**  
  Main Python script used for:
  - data collection
  - API requests
  - cleaning
  - transformation
  - MySQL insertion
  - exploratory analysis

- **sql/schema.sql**  
  SQL script for creating the project database schema

- **sql/analysis.sql**  
  SQL queries used for exploratory and summary analysis

- **data/**  
  Optional folder for storing local outputs or exported files

---

## Possible Next Steps

Potential future improvements for this project:

- Analyze title patterns of high-performing posts
- Explore timing effects (post time / day)
- Group repositories by topic or language
- Build a Power BI dashboard version of the project

---

## Why I Built This Project

I built this project to explore how repository popularity on GitHub relates to attention and discussion on Hacker News.

The goal was not only to compare two platforms, but also to work through a full analysis workflow using real - world API data - including data extraction, cleaning, SQL storage, exploratory analysis, and interpretation.

---