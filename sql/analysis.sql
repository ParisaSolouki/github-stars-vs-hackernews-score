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
