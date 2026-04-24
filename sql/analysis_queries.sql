-- =========================================================
-- Job Market Analytics: SQL Analysis Queries
-- =========================================================

-- 1. Total number of cleaned jobs
SELECT COUNT(*) AS total_jobs
FROM clean_jobs;


-- 2. Total number of job-skill records
SELECT COUNT(*) AS total_job_skill_rows
FROM job_skills;


-- 3. Top 20 most demanded skills
SELECT 
    skill,
    COUNT(*) AS job_count
FROM job_skills
GROUP BY skill
ORDER BY job_count DESC
LIMIT 20;


-- 4. Top 20 cities by number of jobs
SELECT 
    city,
    state,
    COUNT(*) AS job_count
FROM clean_jobs
GROUP BY city, state
ORDER BY job_count DESC
LIMIT 20;


-- 5. Average salary by city
SELECT 
    city,
    state,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_avg), 2) AS avg_salary
FROM clean_jobs
GROUP BY city, state
HAVING COUNT(*) >= 10
ORDER BY avg_salary DESC
LIMIT 20;


-- 6. Average salary by skill
SELECT 
    s.skill,
    COUNT(*) AS job_count,
    ROUND(AVG(j.salary_avg), 2) AS avg_salary
FROM clean_jobs j
JOIN job_skills s
    ON j.job_id = s.job_id
GROUP BY s.skill
HAVING COUNT(*) >= 20
ORDER BY avg_salary DESC
LIMIT 20;


-- 7. Skill demand by sector
SELECT 
    j.sector,
    s.skill,
    COUNT(*) AS job_count
FROM clean_jobs j
JOIN job_skills s
    ON j.job_id = s.job_id
GROUP BY j.sector, s.skill
ORDER BY j.sector, job_count DESC;


-- 8. Average salary by industry
SELECT 
    industry,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_avg), 2) AS avg_salary
FROM clean_jobs
GROUP BY industry
HAVING COUNT(*) >= 10
ORDER BY avg_salary DESC
LIMIT 20;


-- 9. Easy Apply percentage
SELECT 
    easy_apply_bool,
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM clean_jobs
GROUP BY easy_apply_bool;


-- 10. Jobs with the most extracted skills
SELECT 
    job_id,
    job_title,
    clean_company_name,
    city,
    state,
    num_skills
FROM clean_jobs
ORDER BY num_skills DESC
LIMIT 20;


-- 11. Top companies by number of postings
SELECT 
    clean_company_name,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_avg), 2) AS avg_salary
FROM clean_jobs
GROUP BY clean_company_name
ORDER BY job_count DESC
LIMIT 20;


-- 12. Salary distribution buckets
SELECT
    CASE
        WHEN salary_avg < 60000 THEN '<60K'
        WHEN salary_avg >= 60000 AND salary_avg < 90000 THEN '60K-90K'
        WHEN salary_avg >= 90000 AND salary_avg < 120000 THEN '90K-120K'
        WHEN salary_avg >= 120000 AND salary_avg < 150000 THEN '120K-150K'
        WHEN salary_avg >= 150000 THEN '150K+'
        ELSE 'Unknown'
    END AS salary_bucket,
    COUNT(*) AS job_count
FROM clean_jobs
GROUP BY salary_bucket
ORDER BY job_count DESC;