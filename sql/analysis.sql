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

-- =====================================
-- 3) Grouping repos by star ranges
-- =====================================
SELECT 
    CASE 
        WHEN g.stars < 100 THEN 'low'
        WHEN g.stars BETWEEN 100 AND 1000 THEN 'medium'
        ELSE 'high'
    END AS star_group,
    COUNT(*) AS num_posts,
    AVG(h.score) AS avg_hn_score
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id
GROUP BY star_group
ORDER BY avg_hn_score DESC;