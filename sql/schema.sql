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
