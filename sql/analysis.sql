-- =====================================
-- 1) Base dataset: HN score vs GitHub stars
-- =====================================

SELECT 
    h.hn_id,
    h.title,
    h.score,
    g.stars
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id;

-- =====================================
-- 2) Summary metrics
-- =====================================

SELECT 
    AVG(h.score) AS avg_hn_score,
    AVG(g.stars) AS avg_github_stars
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id;