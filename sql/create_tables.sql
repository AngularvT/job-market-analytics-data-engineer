-- =========================================================
-- Create tables for Job Market Analytics Data Pipeline
-- =========================================================

-- Drop existing tables if they already exist.
-- job_skills must be dropped first because it references clean_jobs.
DROP TABLE IF EXISTS job_skills;
DROP TABLE IF EXISTS clean_jobs;

-- =========================================================
-- Main cleaned jobs table
-- One row = one cleaned job posting
-- =========================================================
CREATE TABLE clean_jobs (
    job_id INTEGER PRIMARY KEY,
    job_title TEXT NOT NULL,
    clean_company_name TEXT NOT NULL,
    location TEXT,
    city TEXT,
    state TEXT,
    salary_estimate TEXT,
    salary_min INTEGER,
    salary_max INTEGER,
    salary_avg NUMERIC,
    rating NUMERIC,
    industry TEXT,
    sector TEXT,
    revenue TEXT,
    easy_apply_bool BOOLEAN,
    num_skills INTEGER,
    job_description TEXT
);

-- =========================================================
-- Job-skill mapping table
-- One row = one job-skill pair
-- =========================================================
CREATE TABLE job_skills (
    job_id INTEGER NOT NULL,
    skill TEXT NOT NULL,

    CONSTRAINT fk_job
        FOREIGN KEY (job_id)
        REFERENCES clean_jobs(job_id)
        ON DELETE CASCADE
);

-- =========================================================
-- Indexes for faster analytics queries
-- =========================================================
CREATE INDEX idx_clean_jobs_city
ON clean_jobs(city);

CREATE INDEX idx_clean_jobs_state
ON clean_jobs(state);

CREATE INDEX idx_clean_jobs_industry
ON clean_jobs(industry);

CREATE INDEX idx_job_skills_skill
ON job_skills(skill);

CREATE INDEX idx_job_skills_job_id
ON job_skills(job_id);