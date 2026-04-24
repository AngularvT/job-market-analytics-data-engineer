import os
import pandas as pd


CLEAN_JOBS_PATH = "data/processed/clean_jobs.csv"
JOB_SKILLS_PATH = "data/processed/job_skills.csv"
QUALITY_REPORT_PATH = "data/processed/data_quality_report.txt"


def check_file_exists(file_path: str) -> bool:
    """
    Check whether a required file exists.
    """
    return os.path.exists(file_path)


def load_processed_data():
    """
    Load cleaned jobs and job skills data.
    """
    if not check_file_exists(CLEAN_JOBS_PATH):
        raise FileNotFoundError(
            f"File not found: {CLEAN_JOBS_PATH}\n"
            "Please run src/transform.py first."
        )

    if not check_file_exists(JOB_SKILLS_PATH):
        raise FileNotFoundError(
            f"File not found: {JOB_SKILLS_PATH}\n"
            "Please run src/transform.py first."
        )

    clean_jobs = pd.read_csv(CLEAN_JOBS_PATH)
    job_skills = pd.read_csv(JOB_SKILLS_PATH)

    return clean_jobs, job_skills


def run_quality_checks(clean_jobs: pd.DataFrame, job_skills: pd.DataFrame) -> list:
    """
    Run data quality checks and return a list of report lines.
    """
    report = []

    report.append("========== DATA QUALITY REPORT ==========\n")

    # Basic shape checks
    report.append("1. Dataset Size")
    report.append(f"- clean_jobs rows: {len(clean_jobs)}")
    report.append(f"- clean_jobs columns: {clean_jobs.shape[1]}")
    report.append(f"- job_skills rows: {len(job_skills)}")
    report.append(f"- job_skills columns: {job_skills.shape[1]}\n")

    # Missing value checks
    report.append("2. Missing Values in clean_jobs")
    missing_values = clean_jobs.isnull().sum()
    for col, count in missing_values.items():
        report.append(f"- {col}: {count}")
    report.append("")

    # Duplicate checks
    report.append("3. Duplicate Checks")

    duplicate_job_ids = clean_jobs["job_id"].duplicated().sum()
    report.append(f"- Duplicate job_id count: {duplicate_job_ids}")

    duplicate_job_rows = clean_jobs.duplicated().sum()
    report.append(f"- Exact duplicate rows in clean_jobs: {duplicate_job_rows}")

    if not job_skills.empty:
        duplicate_job_skill_pairs = job_skills.duplicated(subset=["job_id", "skill"]).sum()
    else:
        duplicate_job_skill_pairs = 0

    report.append(f"- Duplicate job_id + skill pairs: {duplicate_job_skill_pairs}\n")

    # Salary checks
    report.append("4. Salary Checks")

    salary_min_missing = clean_jobs["salary_min"].isnull().sum()
    salary_max_missing = clean_jobs["salary_max"].isnull().sum()
    salary_avg_missing = clean_jobs["salary_avg"].isnull().sum()

    report.append(f"- Missing salary_min: {salary_min_missing}")
    report.append(f"- Missing salary_max: {salary_max_missing}")
    report.append(f"- Missing salary_avg: {salary_avg_missing}")

    invalid_salary_range = clean_jobs[
        clean_jobs["salary_min"] > clean_jobs["salary_max"]
    ]

    report.append(f"- Rows where salary_min > salary_max: {len(invalid_salary_range)}")

    very_low_salary = clean_jobs[
        clean_jobs["salary_avg"].notnull() & (clean_jobs["salary_avg"] < 30000)
    ]

    very_high_salary = clean_jobs[
        clean_jobs["salary_avg"].notnull() & (clean_jobs["salary_avg"] > 300000)
    ]

    report.append(f"- Rows with salary_avg < 30000: {len(very_low_salary)}")
    report.append(f"- Rows with salary_avg > 300000: {len(very_high_salary)}")

    report.append("\nSalary Summary:")
    salary_summary = clean_jobs[["salary_min", "salary_max", "salary_avg"]].describe()
    report.append(str(salary_summary))
    report.append("")

    # Text field checks
    report.append("5. Required Text Field Checks")

    empty_titles = clean_jobs["job_title"].isnull().sum()
    empty_companies = clean_jobs["clean_company_name"].isnull().sum()
    empty_descriptions = clean_jobs["job_description"].isnull().sum()

    report.append(f"- Missing job_title: {empty_titles}")
    report.append(f"- Missing clean_company_name: {empty_companies}")
    report.append(f"- Missing job_description: {empty_descriptions}\n")

    # Location checks
    report.append("6. Location Checks")

    missing_city = clean_jobs["city"].isnull().sum()
    missing_state = clean_jobs["state"].isnull().sum()
    unknown_city = (clean_jobs["city"] == "Unknown").sum()
    unknown_state = (clean_jobs["state"] == "Unknown").sum()

    report.append(f"- Missing city: {missing_city}")
    report.append(f"- Missing state: {missing_state}")
    report.append(f"- Unknown city: {unknown_city}")
    report.append(f"- Unknown state: {unknown_state}\n")

    # Skill extraction checks
    report.append("7. Skill Extraction Checks")

    jobs_with_zero_skills = (clean_jobs["num_skills"] == 0).sum()
    jobs_with_skills = (clean_jobs["num_skills"] > 0).sum()

    report.append(f"- Jobs with zero extracted skills: {jobs_with_zero_skills}")
    report.append(f"- Jobs with at least one extracted skill: {jobs_with_skills}")

    if len(clean_jobs) > 0:
        skill_coverage_rate = jobs_with_skills / len(clean_jobs)
    else:
        skill_coverage_rate = 0

    report.append(f"- Skill coverage rate: {skill_coverage_rate:.2%}")

    report.append("\nSkill Count Summary:")
    report.append(str(clean_jobs["num_skills"].describe()))

    report.append("\nTop 20 Skills:")
    if not job_skills.empty:
        top_skills = job_skills["skill"].value_counts().head(20)
        report.append(str(top_skills))
    else:
        report.append("No skills extracted.")

    report.append("")

    # Referential integrity check
    report.append("8. Referential Integrity Checks")

    clean_job_ids = set(clean_jobs["job_id"])
    skill_job_ids = set(job_skills["job_id"]) if not job_skills.empty else set()

    invalid_skill_job_ids = skill_job_ids - clean_job_ids

    report.append(
        f"- job_id values in job_skills but not in clean_jobs: {len(invalid_skill_job_ids)}"
    )

    jobs_without_skill_rows = clean_job_ids - skill_job_ids

    report.append(
        f"- jobs in clean_jobs without matching rows in job_skills: {len(jobs_without_skill_rows)}"
    )

    report.append("\n========== END OF REPORT ==========")

    return report


def save_quality_report(report_lines: list, output_path: str) -> None:
    """
    Save quality report to a text file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for line in report_lines:
            f.write(str(line) + "\n")

    print(f"Data quality report saved to: {output_path}")


def main():
    print("Starting data quality checks...")

    clean_jobs, job_skills = load_processed_data()

    report_lines = run_quality_checks(clean_jobs, job_skills)

    for line in report_lines:
        print(line)

    save_quality_report(report_lines, QUALITY_REPORT_PATH)

    print("\nData quality checks completed successfully.")


if __name__ == "__main__":
    main()