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

-- =====================================
-- 4) Distribution check: score vs stars
-- =====================================

SELECT 
    h.score,
    g.stars
FROM hn_posts h
JOIN github_repos g
    ON h.repo_id = g.repo_id
ORDER BY g.stars DESC;


-- =====================================
-- 5) Outliers using quartiles (data-driven)
-- =====================================

SELECT *
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
