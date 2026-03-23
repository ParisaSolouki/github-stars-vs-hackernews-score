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