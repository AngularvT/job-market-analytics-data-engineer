import os
import re
from typing import List, Optional

import pandas as pd


RAW_DATA_PATH = "data/raw/DataEngineer.csv"
CLEAN_JOBS_OUTPUT_PATH = "data/processed/clean_jobs.csv"
JOB_SKILLS_OUTPUT_PATH = "data/processed/job_skills.csv"


# A simple skill dictionary for rule-based extraction.
# This is explainable and easy to modify later.
SKILL_KEYWORDS = [
    "Python",
    "SQL",
    "PostgreSQL",
    "MySQL",
    "SQL Server",
    "Oracle",
    "MongoDB",
    "NoSQL",
    "Spark",
    "Hadoop",
    "Hive",
    "Kafka",
    "Airflow",
    "dbt",
    "Snowflake",
    "Redshift",
    "BigQuery",
    "AWS",
    "Azure",
    "GCP",
    "Docker",
    "Kubernetes",
    "Tableau",
    "Power BI",
    "Looker",
    "Pandas",
    "NumPy",
    "ETL",
    "ELT",
    "Data Warehouse",
    "Data Lake",
    "Data Modeling",
    "Machine Learning",
    "Git",
    "Linux",
    "Java",
    "Scala",
    "R",
]


def clean_company_name(company_name: str) -> str:
    """
    Clean company name by removing rating text after line breaks.

    Example:
        "Sagence\\n4.5" -> "Sagence"
    """
    if pd.isna(company_name):
        return "Unknown"

    company_name = str(company_name).strip()

    # Many rows store company name and rating together, separated by a newline.
    company_name = company_name.split("\n")[0].strip()

    return company_name


def parse_salary_min(salary_text: str) -> Optional[int]:
    """
    Extract minimum salary from salary estimate.

    Example:
        "$80K-$150K (Glassdoor est.)" -> 80000
    """
    if pd.isna(salary_text):
        return None

    salary_text = str(salary_text)

    # Find salary numbers like 80K, 150K.
    numbers = re.findall(r"\$?(\d+)K", salary_text)

    if len(numbers) >= 1:
        return int(numbers[0]) * 1000

    return None


def parse_salary_max(salary_text: str) -> Optional[int]:
    """
    Extract maximum salary from salary estimate.

    Example:
        "$80K-$150K (Glassdoor est.)" -> 150000
    """
    if pd.isna(salary_text):
        return None

    salary_text = str(salary_text)

    numbers = re.findall(r"\$?(\d+)K", salary_text)

    if len(numbers) >= 2:
        return int(numbers[1]) * 1000

    # If only one salary number exists, use it as both min and max.
    if len(numbers) == 1:
        return int(numbers[0]) * 1000

    return None


def parse_city(location: str) -> str:
    """
    Extract city from location.

    Example:
        "New York, NY" -> "New York"
    """
    if pd.isna(location):
        return "Unknown"

    location = str(location).strip()

    if "," in location:
        return location.split(",")[0].strip()

    return location


def parse_state(location: str) -> str:
    """
    Extract state from location.

    Example:
        "New York, NY" -> "NY"
    """
    if pd.isna(location):
        return "Unknown"

    location = str(location).strip()

    if "," in location:
        return location.split(",")[-1].strip()

    return "Unknown"


def normalize_easy_apply(value) -> bool:
    """
    Convert Easy Apply field into boolean.

    In this dataset:
        True means easy apply is available.
        -1 usually means missing or not available.
    """
    if str(value).strip().lower() == "true":
        return True

    return False


def extract_skills(description: str) -> List[str]:
    """
    Extract technical skills from job description using keyword matching.

    This is a simple, explainable rule-based extractor.
    """
    if pd.isna(description):
        return []

    description_lower = str(description).lower()
    found_skills = []

    for skill in SKILL_KEYWORDS:
        skill_lower = skill.lower()

        # Use word boundaries to avoid partial matching.
        # Example: match "SQL", but avoid matching it inside unrelated words.
        pattern = r"\b" + re.escape(skill_lower) + r"\b"

        if re.search(pattern, description_lower):
            found_skills.append(skill)

    return sorted(set(found_skills))


def transform_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw job posting data into an analysis-ready jobs table.
    """
    clean_df = df.copy()

    # IMPORTANT:
    # Remove duplicate rows before creating list-type columns.
    # Pandas drop_duplicates() cannot handle list values because lists are unhashable.
    clean_df = clean_df.drop_duplicates().reset_index(drop=True)

    # Add a stable job_id after deduplication.
    clean_df.insert(0, "job_id", range(1, len(clean_df) + 1))

    # Clean company name.
    clean_df["clean_company_name"] = clean_df["Company Name"].apply(clean_company_name)

    # Parse salary range into numeric columns.
    clean_df["salary_min"] = clean_df["Salary Estimate"].apply(parse_salary_min)
    clean_df["salary_max"] = clean_df["Salary Estimate"].apply(parse_salary_max)
    clean_df["salary_avg"] = (clean_df["salary_min"] + clean_df["salary_max"]) / 2

    # Split location into city and state.
    clean_df["city"] = clean_df["Location"].apply(parse_city)
    clean_df["state"] = clean_df["Location"].apply(parse_state)

    # Normalize Easy Apply field.
    clean_df["easy_apply_bool"] = clean_df["Easy Apply"].apply(normalize_easy_apply)

    # Extract skills from job description.
    clean_df["extracted_skills"] = clean_df["Job Description"].apply(extract_skills)
    clean_df["num_skills"] = clean_df["extracted_skills"].apply(len)

    # Select useful columns for the cleaned jobs table.
    clean_jobs = clean_df[
        [
            "job_id",
            "Job Title",
            "clean_company_name",
            "Location",
            "city",
            "state",
            "Salary Estimate",
            "salary_min",
            "salary_max",
            "salary_avg",
            "Rating",
            "Industry",
            "Sector",
            "Revenue",
            "easy_apply_bool",
            "num_skills",
            "Job Description",
        ]
    ].copy()

    # Rename columns to snake_case for database friendliness.
    clean_jobs = clean_jobs.rename(
        columns={
            "Job Title": "job_title",
            "Location": "location",
            "Salary Estimate": "salary_estimate",
            "Rating": "rating",
            "Industry": "industry",
            "Sector": "sector",
            "Revenue": "revenue",
            "Job Description": "job_description",
        }
    )

    return clean_jobs


def build_job_skills_table(clean_jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Create a normalized job_skills table.

    One job can have many skills, so this creates one row per job-skill pair.

    Example:
        job_id | skill
        1      | Python
        1      | SQL
        1      | Airflow
    """
    rows = []

    for _, row in clean_jobs.iterrows():
        job_id = row["job_id"]
        description = row["job_description"]
        skills = extract_skills(description)

        for skill in skills:
            rows.append(
                {
                    "job_id": job_id,
                    "skill": skill,
                }
            )

    job_skills_df = pd.DataFrame(rows)

    # If no skills are extracted, return an empty DataFrame with expected columns.
    if job_skills_df.empty:
        job_skills_df = pd.DataFrame(columns=["job_id", "skill"])

    return job_skills_df


def save_outputs(clean_jobs: pd.DataFrame, job_skills: pd.DataFrame) -> None:
    """
    Save transformed outputs to data/processed.
    """
    os.makedirs("data/processed", exist_ok=True)

    clean_jobs.to_csv(CLEAN_JOBS_OUTPUT_PATH, index=False)
    job_skills.to_csv(JOB_SKILLS_OUTPUT_PATH, index=False)

    print(f"Clean jobs saved to: {CLEAN_JOBS_OUTPUT_PATH}")
    print(f"Job skills saved to: {JOB_SKILLS_OUTPUT_PATH}")


def main():
    print("Starting transformation...")

    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError(
            f"File not found: {RAW_DATA_PATH}\n"
            "Please make sure DataEngineer.csv is inside data/raw/"
        )

    raw_df = pd.read_csv(RAW_DATA_PATH)

    clean_jobs = transform_jobs(raw_df)
    job_skills = build_job_skills_table(clean_jobs)

    save_outputs(clean_jobs, job_skills)

    print("\n========== TRANSFORMATION SUMMARY ==========")
    print(f"Raw rows: {len(raw_df)}")
    print(f"Clean job rows after deduplication: {len(clean_jobs)}")
    print(f"Removed duplicate rows: {len(raw_df) - len(clean_jobs)}")
    print(f"Job-skill rows: {len(job_skills)}")

    print("\n========== CLEAN JOBS PREVIEW ==========")
    print(clean_jobs.head())

    print("\n========== TOP 20 SKILLS ==========")
    if not job_skills.empty:
        print(job_skills["skill"].value_counts().head(20))
    else:
        print("No skills extracted.")

    print("\n========== SALARY CHECK ==========")
    print(clean_jobs[["salary_min", "salary_max", "salary_avg"]].describe())

    print("\nTransformation completed successfully.")
 

if __name__ == "__main__":
    main()