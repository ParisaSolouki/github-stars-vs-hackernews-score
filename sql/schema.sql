-- =====================================
-- Schema: Hacker News & GitHub Tracker
-- =====================================

CREATE DATABASE IF NOT EXISTS hn_github_tracker;

USE hn_github_tracker;


-- =====================================
-- 1) Table: github_repos
-- =====================================

CREATE TABLE IF NOT EXISTS github_repos (
  repo_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  full_name VARCHAR(200) NOT NULL UNIQUE,
  owner VARCHAR(100) NOT NULL,
  repo VARCHAR(100) NOT NULL,
  stars INT UNSIGNED,
  language VARCHAR(50),
  forks INT UNSIGNED,
  open_issues INT UNSIGNED,
  updated_at DATETIME,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =====================================
-- 2) Table: hn_posts
-- =====================================

CREATE TABLE IF NOT EXISTS hn_posts (
  hn_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  title VARCHAR(300) NOT NULL,
  author VARCHAR(80) NOT NULL,
  comments INT UNSIGNED,
  score INT UNSIGNED,
  post_time DATETIME NOT NULL,
  url VARCHAR(2048),
  repo_id BIGINT UNSIGNED,
  full_name VARCHAR(200)
);


-- =====================================
-- 3) Indexes
-- =====================================

CREATE INDEX idx_hn_posts_repo_id ON hn_posts(repo_id);
CREATE INDEX idx_hn_posts_full_name ON hn_posts(full_name);
CREATE INDEX idx_github_full_name ON github_repos(full_name);



