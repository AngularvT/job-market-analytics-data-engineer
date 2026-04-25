import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text


# =========================================================
# Page configuration
# =========================================================
st.set_page_config(
    page_title="Job Market Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)


# =========================================================
# Database connection
# =========================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://zhaolanboer@localhost:5432/job_market_db"
)


@st.cache_resource
def get_engine():
    """
    Create and cache a SQLAlchemy engine.
    """
    return create_engine(DATABASE_URL)


@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    """
    Run a SQL query and return the result as a pandas DataFrame.
    """
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    return df


# =========================================================
# SQL queries
# =========================================================
TOTAL_JOBS_QUERY = """
SELECT COUNT(*) AS total_jobs
FROM clean_jobs;
"""

TOTAL_SKILL_ROWS_QUERY = """
SELECT COUNT(*) AS total_skill_rows
FROM job_skills;
"""

AVG_SALARY_QUERY = """
SELECT ROUND(AVG(salary_avg), 2) AS avg_salary
FROM clean_jobs;
"""

TOP_SKILLS_QUERY = """
SELECT 
    skill,
    COUNT(*) AS job_count
FROM job_skills
GROUP BY skill
ORDER BY job_count DESC
LIMIT 20;
"""

TOP_CITIES_QUERY = """
SELECT 
    city,
    state,
    COUNT(*) AS job_count
FROM clean_jobs
GROUP BY city, state
ORDER BY job_count DESC
LIMIT 20;
"""

AVG_SALARY_BY_SKILL_QUERY = """
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
"""

SALARY_BUCKET_QUERY = """
WITH salary_buckets AS (
    SELECT
        CASE
            WHEN salary_avg < 60000 THEN '<60K'
            WHEN salary_avg >= 60000 AND salary_avg < 90000 THEN '60K-90K'
            WHEN salary_avg >= 90000 AND salary_avg < 120000 THEN '90K-120K'
            WHEN salary_avg >= 120000 AND salary_avg < 150000 THEN '120K-150K'
            WHEN salary_avg >= 150000 THEN '150K+'
            ELSE 'Unknown'
        END AS salary_bucket,
        CASE
            WHEN salary_avg < 60000 THEN 1
            WHEN salary_avg >= 60000 AND salary_avg < 90000 THEN 2
            WHEN salary_avg >= 90000 AND salary_avg < 120000 THEN 3
            WHEN salary_avg >= 120000 AND salary_avg < 150000 THEN 4
            WHEN salary_avg >= 150000 THEN 5
            ELSE 6
        END AS bucket_order
    FROM clean_jobs
)
SELECT
    salary_bucket,
    COUNT(*) AS job_count
FROM salary_buckets
GROUP BY salary_bucket, bucket_order
ORDER BY bucket_order;
"""

TOP_COMPANIES_QUERY = """
SELECT 
    clean_company_name,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_avg), 2) AS avg_salary
FROM clean_jobs
GROUP BY clean_company_name
ORDER BY job_count DESC
LIMIT 20;
"""

EASY_APPLY_QUERY = """
SELECT 
    easy_apply_bool,
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM clean_jobs
GROUP BY easy_apply_bool;
"""

JOBS_PREVIEW_QUERY = """
SELECT 
    job_id,
    job_title,
    clean_company_name,
    city,
    state,
    salary_avg,
    rating,
    industry,
    sector,
    easy_apply_bool,
    num_skills
FROM clean_jobs
ORDER BY job_id
LIMIT 100;
"""


# =========================================================
# Dashboard title
# =========================================================
st.title("📊 Job Market Analytics Dashboard")
st.markdown(
    """
    This dashboard analyzes Data Engineer job postings using a PostgreSQL-backed data pipeline.
    
    The pipeline follows:
    
    **Raw CSV → Extract → Transform → Data Quality Check → PostgreSQL Load → SQL Analytics → Dashboard**
    """
)


# =========================================================
# Load data from PostgreSQL
# =========================================================
try:
    total_jobs_df = run_query(TOTAL_JOBS_QUERY)
    total_skill_rows_df = run_query(TOTAL_SKILL_ROWS_QUERY)
    avg_salary_df = run_query(AVG_SALARY_QUERY)

    top_skills_df = run_query(TOP_SKILLS_QUERY)
    top_cities_df = run_query(TOP_CITIES_QUERY)
    avg_salary_by_skill_df = run_query(AVG_SALARY_BY_SKILL_QUERY)
    salary_bucket_df = run_query(SALARY_BUCKET_QUERY)
    top_companies_df = run_query(TOP_COMPANIES_QUERY)
    easy_apply_df = run_query(EASY_APPLY_QUERY)
    jobs_preview_df = run_query(JOBS_PREVIEW_QUERY)

except Exception as e:
    st.error("Failed to connect to PostgreSQL or load data.")
    st.write("Please make sure PostgreSQL is running and `src/load.py` has already loaded the data.")
    st.exception(e)
    st.stop()


# =========================================================
# KPI cards
# =========================================================
st.subheader("Overview")

col1, col2, col3 = st.columns(3)

with col1:
    total_jobs = int(total_jobs_df["total_jobs"].iloc[0])
    st.metric("Total Cleaned Jobs", f"{total_jobs:,}")

with col2:
    total_skill_rows = int(total_skill_rows_df["total_skill_rows"].iloc[0])
    st.metric("Job-Skill Records", f"{total_skill_rows:,}")

with col3:
    avg_salary = float(avg_salary_df["avg_salary"].iloc[0])
    st.metric("Average Salary", f"${avg_salary:,.0f}")


st.divider()


# =========================================================
# Sidebar filters
# =========================================================
st.sidebar.header("Dashboard Controls")

chart_limit = st.sidebar.slider(
    "Number of rows to show in charts",
    min_value=5,
    max_value=20,
    value=15
)


# =========================================================
# Top skills and top cities
# =========================================================
left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Top Required Skills")

    skills_chart_df = top_skills_df.head(chart_limit)

    fig_skills = px.bar(
        skills_chart_df,
        x="job_count",
        y="skill",
        orientation="h",
        title="Most Demanded Data Engineering Skills",
        labels={
            "job_count": "Number of Job Postings",
            "skill": "Skill"
        }
    )

    fig_skills.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_skills, use_container_width=True)

    st.dataframe(skills_chart_df, use_container_width=True)


with right_col:
    st.subheader("Top Cities by Job Count")

    cities_chart_df = top_cities_df.head(chart_limit).copy()
    cities_chart_df["city_state"] = cities_chart_df["city"] + ", " + cities_chart_df["state"]

    fig_cities = px.bar(
        cities_chart_df,
        x="job_count",
        y="city_state",
        orientation="h",
        title="Cities with the Most Data Engineer Jobs",
        labels={
            "job_count": "Number of Job Postings",
            "city_state": "City"
        }
    )

    fig_cities.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_cities, use_container_width=True)

    st.dataframe(cities_chart_df, use_container_width=True)


st.divider()


# =========================================================
# Salary analysis
# =========================================================
left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Average Salary by Skill")

    salary_skill_chart_df = avg_salary_by_skill_df.head(chart_limit)

    fig_salary_skill = px.bar(
        salary_skill_chart_df,
        x="avg_salary",
        y="skill",
        orientation="h",
        title="Highest Average Salary by Skill",
        labels={
            "avg_salary": "Average Salary",
            "skill": "Skill"
        }
    )

    fig_salary_skill.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_salary_skill, use_container_width=True)

    st.dataframe(salary_skill_chart_df, use_container_width=True)


with right_col:
    st.subheader("Salary Distribution")

    fig_salary_bucket = px.bar(
        salary_bucket_df,
        x="salary_bucket",
        y="job_count",
        title="Job Count by Salary Range",
        labels={
            "salary_bucket": "Salary Range",
            "job_count": "Number of Job Postings"
        }
    )

    st.plotly_chart(fig_salary_bucket, use_container_width=True)

    st.dataframe(salary_bucket_df, use_container_width=True)


st.divider()


# =========================================================
# Company and Easy Apply analysis
# =========================================================
left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Top Hiring Companies")

    companies_chart_df = top_companies_df.head(chart_limit)

    fig_companies = px.bar(
        companies_chart_df,
        x="job_count",
        y="clean_company_name",
        orientation="h",
        title="Companies with the Most Job Postings",
        labels={
            "job_count": "Number of Job Postings",
            "clean_company_name": "Company"
        }
    )

    fig_companies.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_companies, use_container_width=True)

    st.dataframe(companies_chart_df, use_container_width=True)


with right_col:
    st.subheader("Easy Apply Distribution")

    easy_apply_display_df = easy_apply_df.copy()
    easy_apply_display_df["easy_apply_label"] = easy_apply_display_df["easy_apply_bool"].apply(
        lambda x: "Easy Apply" if x else "Not Easy Apply"
    )

    fig_easy_apply = px.pie(
        easy_apply_display_df,
        names="easy_apply_label",
        values="job_count",
        title="Easy Apply vs Not Easy Apply"
    )

    st.plotly_chart(fig_easy_apply, use_container_width=True)

    st.dataframe(easy_apply_display_df, use_container_width=True)


st.divider()


# =========================================================
# Data preview
# =========================================================
st.subheader("Cleaned Job Data Preview")

st.markdown(
    """
    This table shows a preview of the cleaned job-level dataset loaded from PostgreSQL.
    """
)

st.dataframe(jobs_preview_df, use_container_width=True)


# =========================================================
# Footer
# =========================================================
st.markdown("---")
st.markdown(
    """
    **Project:** Job Market Analytics Data Pipeline  
    **Tech Stack:** Python, Pandas, PostgreSQL, SQLAlchemy, Streamlit, Plotly  
    """
)