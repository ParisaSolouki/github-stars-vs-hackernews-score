-- =====================================
-- 1) Base dataset: HN posts joined with GitHub repo metadata
-- =====================================

SELECT 
    h.hn_id,
    h.title,
    h.author,
    h.score,
    h.comments,
    h.post_time,
    g.repo_id,
    g.full_name,
    g.language,
    g.stars,
    g.forks,
    g.open_issues,
    g.updated_at
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id;

    -- =====================================
-- 2) Summary metrics
-- =====================================

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT h.hn_id) AS total_hn_posts,
    COUNT(DISTINCT g.repo_id) AS total_github_repos,
    AVG(h.score) AS avg_hn_score,
    MIN(h.score) AS min_hn_score,
    MAX(h.score) AS max_hn_score,
    AVG(g.stars) AS avg_github_stars,
    MIN(g.stars) AS min_github_stars,
    MAX(g.stars) AS max_github_stars
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id;

-- =====================================
-- 3) Data-driven grouping using star quantiles
-- =====================================

SELECT 
    CASE 
        WHEN stars_group = 1 THEN 'low'
        WHEN stars_group = 2 THEN 'medium'
        ELSE 'high'
    END AS star_group,
    COUNT(*) AS num_repos,
    AVG(stars) AS avg_github_stars,
    AVG(score) AS avg_hn_score,
    MIN(score) AS min_hn_score,
    MAX(score) AS max_hn_score
FROM (
    SELECT 
        h.score,
        g.stars,
        NTILE(3) OVER (ORDER BY g.stars ASC) AS stars_group
    FROM hn_posts h
    JOIN github_repos g
        ON h.repo_id = g.repo_id
) AS ranked_data
GROUP BY stars_group
ORDER BY stars_group;

-- =====================================
-- 4) Outlier detection using quartiles
-- =====================================
-- low stars but high score

SELECT 
    title,
    score,
    stars
FROM (
    SELECT
        h.title,
        h.score,
        g.stars,
        NTILE(4) OVER (ORDER BY g.stars ASC) AS stars_quartile,
        NTILE(4) OVER (ORDER BY h.score ASC) AS score_quartile
    FROM hn_posts h
    JOIN github_repos g
        ON h.repo_id = g.repo_id
) AS ranked_data
WHERE stars_quartile = 1
  AND score_quartile = 4
ORDER BY score DESC;



-- high stars but low score

SELECT 
    title,
    score,
    stars
FROM (
    SELECT
        h.title,
        h.score,
        g.stars,
        NTILE(4) OVER (ORDER BY g.stars ASC) AS stars_quartile,
        NTILE(4) OVER (ORDER BY h.score ASC) AS score_quartile
    FROM hn_posts h
    JOIN github_repos g
        ON h.repo_id = g.repo_id
) AS ranked_data
WHERE stars_quartile = 4
  AND score_quartile = 1
ORDER BY stars DESC;

-- =====================================
-- 5) Quartile cross-tab: score vs stars
-- =====================================

SELECT
    stars_quartile,
    score_quartile,
    COUNT(*) AS num_repos
FROM (
    SELECT
        h.title,
        h.score,
        g.stars,
        NTILE(4) OVER (ORDER BY g.stars ASC) AS stars_quartile,
        NTILE(4) OVER (ORDER BY h.score ASC) AS score_quartile
    FROM hn_posts h
    JOIN github_repos g
        ON h.repo_id = g.repo_id
) AS ranked_data
GROUP BY stars_quartile, score_quartile
ORDER BY stars_quartile, score_quartile;

-- =====================================
-- 6) Top performing repositories (repo-level)
-- =====================================

SELECT 
    g.full_name,
    COUNT(h.hn_id) AS num_posts,
    AVG(h.score) AS avg_hn_score,
    MAX(h.score) AS max_hn_score,
    AVG(g.stars) AS avg_github_stars
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id
GROUP BY g.repo_id, g.full_name
ORDER BY avg_hn_score DESC
LIMIT 10;


-- =====================================
-- 7) Distribution dataset for analysis (Python)
-- =====================================

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
