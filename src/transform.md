# Transform Pipeline README

## 1. Overview

This document explains the `src/transform.py` script in the **Job Market Analytics Data Pipeline** project.

The purpose of this script is to transform raw Data Engineer job posting data into clean, structured, and analysis-ready datasets.

The raw dataset comes from:

```text
data/raw/DataEngineer.csv
```

The script generates two transformed output files:

```text
data/processed/clean_jobs.csv
data/processed/job_skills.csv
```

These output files will later be used for database loading, SQL analytics, and dashboard visualization.

---

## 2. Role in the ETL Pipeline

This project follows a standard ETL workflow:

```text
Extract  →  Transform  →  Load
```

The `transform.py` script represents the **Transform** step.

### ETL Step Breakdown

| Step | Description | Related File |
|---|---|---|
| Extract | Read the raw CSV file and inspect the dataset | `src/extract.py` |
| Transform | Clean, standardize, and structure the raw job posting data | `src/transform.py` |
| Load | Load cleaned data into PostgreSQL | `src/load.py` |

The transform step is important because raw job posting data is messy and not directly suitable for analytics.

---

## 3. Input Data

The input file is:

```text
data/raw/DataEngineer.csv
```

The raw dataset contains Data Engineer job postings with fields such as:

| Raw Column | Description |
|---|---|
| `Job Title` | Job title from the original posting |
| `Salary Estimate` | Salary range stored as text |
| `Job Description` | Full unstructured job description |
| `Rating` | Company rating |
| `Company Name` | Company name, sometimes combined with rating text |
| `Location` | Job location |
| `Headquarters` | Company headquarters |
| `Size` | Company size |
| `Founded` | Company founding year |
| `Type of ownership` | Company ownership type |
| `Industry` | Industry category |
| `Sector` | Sector category |
| `Revenue` | Company revenue range |
| `Competitors` | Competitor companies |
| `Easy Apply` | Whether the job supports Easy Apply |

Some of these columns need to be cleaned before analysis.

For example:

```text
Company Name: "Sagence\n4.5"
Salary Estimate: "$80K-$150K (Glassdoor est.)"
Location: "New York, NY"
Easy Apply: "-1"
```

The transform script converts these messy values into cleaner and more useful fields.

---

## 4. Output Data

The transform script creates two output files.

### 4.1 `clean_jobs.csv`

Path:

```text
data/processed/clean_jobs.csv
```

This is the main cleaned job table.

It contains one row per job posting.

Important columns include:

| Column | Description |
|---|---|
| `job_id` | Unique ID created for each job posting |
| `job_title` | Job title |
| `clean_company_name` | Cleaned company name |
| `location` | Original location string |
| `city` | Parsed city |
| `state` | Parsed state |
| `salary_estimate` | Original salary text |
| `salary_min` | Minimum salary as a numeric value |
| `salary_max` | Maximum salary as a numeric value |
| `salary_avg` | Average salary calculated from min and max |
| `rating` | Company rating |
| `industry` | Industry from raw data |
| `sector` | Sector from raw data |
| `revenue` | Revenue category |
| `easy_apply_bool` | Boolean version of Easy Apply |
| `num_skills` | Number of extracted technical skills |
| `job_description` | Original job description text |

---

### 4.2 `job_skills.csv`

Path:

```text
data/processed/job_skills.csv
```

This is a normalized job-skill mapping table.

It contains one row per job-skill pair.

Example:

| job_id | skill |
|---|---|
| 1 | Python |
| 1 | SQL |
| 1 | Airflow |
| 2 | Tableau |
| 2 | SQL |

This structure is useful because one job can require many skills.

Instead of storing all skills in one cell, the script creates multiple rows so that SQL analysis becomes easier.

Example SQL analysis:

```sql
SELECT skill, COUNT(*) AS job_count
FROM job_skills
GROUP BY skill
ORDER BY job_count DESC;
```

This query returns the most frequently required technical skills.

---

## 5. Main Transformations

The `transform.py` script performs several important data cleaning and transformation steps.

---

### 5.1 Remove Duplicate Rows

The raw dataset may contain duplicate job postings.

The script removes exact duplicate rows using:

```python
clean_df = clean_df.drop_duplicates().reset_index(drop=True)
```

This step is done before creating list-type columns such as `extracted_skills`.

This is important because pandas `drop_duplicates()` cannot handle columns containing Python lists. Lists are unhashable, so duplicate removal must happen before skill lists are created.

---

### 5.2 Create `job_id`

The raw dataset does not include a stable unique job ID.

After deduplication, the script creates a new `job_id`:

```python
clean_df.insert(0, "job_id", range(1, len(clean_df) + 1))
```

Example:

| job_id | job_title |
|---|---|
| 1 | Data Engineer |
| 2 | Senior Data Engineer |
| 3 | Data Engineers |

The `job_id` is important because it connects the main job table with the job-skill mapping table.

---

### 5.3 Clean Company Names

Some company names include rating information after a line break.

Example raw value:

```text
Sagence
4.5
```

The script keeps only the first line:

```text
Sagence
```

Function used:

```python
def clean_company_name(company_name: str) -> str:
    if pd.isna(company_name):
        return "Unknown"

    company_name = str(company_name).strip()
    company_name = company_name.split("\n")[0].strip()

    return company_name
```

This produces the column:

```text
clean_company_name
```

This makes company-level analysis cleaner and more reliable.

---

### 5.4 Parse Salary Information

The original salary estimate is stored as text.

Example:

```text
$80K-$150K (Glassdoor est.)
```

The script extracts numeric salary values:

```text
salary_min = 80000
salary_max = 150000
salary_avg = 115000
```

Functions used:

```python
parse_salary_min()
parse_salary_max()
```

The script uses a regular expression to find salary numbers:

```python
re.findall(r"\$?(\d+)K", salary_text)
```

This finds values such as:

```text
80K
150K
```

Then it converts them into full salary numbers by multiplying by 1,000.

This allows later analysis such as:

- Average salary by city
- Average salary by skill
- Average salary by industry
- Salary distribution across job titles

---

### 5.5 Split Location into City and State

The original `Location` field is stored as a single string.

Example:

```text
New York, NY
```

The script splits it into two fields:

```text
city = New York
state = NY
```

Functions used:

```python
parse_city()
parse_state()
```

This makes it easier to analyze job demand by location.

Example analysis questions:

- Which cities have the most Data Engineer jobs?
- Which states have higher average salaries?
- Which locations have more Easy Apply jobs?

---

### 5.6 Normalize Easy Apply

The raw `Easy Apply` column may contain values such as:

```text
True
-1
```

The script converts this field into a boolean column:

```text
easy_apply_bool
```

Rules:

| Raw Value | Transformed Value |
|---|---|
| `True` | `True` |
| `-1` | `False` |
| Other values | `False` |

Function used:

```python
normalize_easy_apply()
```

This makes it easier to calculate the percentage of jobs that support Easy Apply.

---

### 5.7 Extract Technical Skills

The `Job Description` column contains long unstructured text.

The script extracts technical skills from this text using a rule-based keyword matching approach.

The skill dictionary includes common data engineering skills such as:

```text
Python
SQL
PostgreSQL
MySQL
SQL Server
Oracle
MongoDB
NoSQL
Spark
Hadoop
Hive
Kafka
Airflow
dbt
Snowflake
Redshift
BigQuery
AWS
Azure
GCP
Docker
Kubernetes
Tableau
Power BI
Looker
Pandas
NumPy
ETL
ELT
Data Warehouse
Data Lake
Data Modeling
Machine Learning
Git
Linux
Java
Scala
R
```

Example job description:

```text
We are looking for a Data Engineer with Python, SQL, Airflow, Spark, and AWS experience.
```

Extracted skills:

```text
Python
SQL
Airflow
Spark
AWS
```

Function used:

```python
extract_skills()
```

This transformation converts unstructured job descriptions into structured skill data.

This is one of the most important parts of the project because it enables skill-demand analysis.

---

## 6. Why Create a Separate `job_skills.csv` Table?

A single job posting can require multiple skills.

For example:

| job_id | job_title | skills |
|---|---|---|
| 1 | Data Engineer | Python, SQL, Airflow, Spark |

This format is not ideal for SQL analytics because the `skills` field contains multiple values in one cell.

Instead, the script creates a normalized table:

| job_id | skill |
|---|---|
| 1 | Python |
| 1 | SQL |
| 1 | Airflow |
| 1 | Spark |

This makes analysis much easier.

For example, we can count skill frequency:

```sql
SELECT skill, COUNT(*) AS job_count
FROM job_skills
GROUP BY skill
ORDER BY job_count DESC;
```

We can also join `job_skills.csv` with `clean_jobs.csv` to analyze salary by skill:

```sql
SELECT 
    s.skill,
    AVG(j.salary_avg) AS avg_salary
FROM clean_jobs j
JOIN job_skills s
    ON j.job_id = s.job_id
GROUP BY s.skill
ORDER BY avg_salary DESC;
```

This is a common data engineering pattern: separating entities and relationships into structured tables.

---

## 7. Code Structure

The script is organized into reusable functions.

| Function | Purpose |
|---|---|
| `clean_company_name()` | Removes rating text from company names |
| `parse_salary_min()` | Extracts minimum salary from salary text |
| `parse_salary_max()` | Extracts maximum salary from salary text |
| `parse_city()` | Extracts city from location |
| `parse_state()` | Extracts state from location |
| `normalize_easy_apply()` | Converts Easy Apply into a boolean value |
| `extract_skills()` | Extracts technical skills from job descriptions |
| `transform_jobs()` | Creates the cleaned job table |
| `build_job_skills_table()` | Creates the normalized job-skill table |
| `save_outputs()` | Saves transformed data to CSV files |
| `main()` | Runs the full transformation workflow |

---

## 8. How the Script Runs

When running:

```bash
python3 src/transform.py
```

The script executes the following workflow:

```text
1. Check whether data/raw/DataEngineer.csv exists
2. Read the raw CSV file
3. Remove duplicate rows
4. Create job_id
5. Clean company names
6. Parse salary fields
7. Split location into city and state
8. Normalize Easy Apply values
9. Extract technical skills from job descriptions
10. Create clean_jobs.csv
11. Create job_skills.csv
12. Print transformation summary
```

---

## 9. How to Run the Script

From the project root directory, run:

```bash
cd /Users/zhaolanboer/Documents/GitHub/job-market-analytics-data-engineer
python3 src/transform.py
```

After running the script, check the processed data folder:

```bash
ls data/processed
```

Expected output:

```text
clean_jobs.csv
job_skills.csv
raw_preview.csv
```

---

## 10. Expected Terminal Output

A successful run should print something like:

```text
Starting transformation...
Clean jobs saved to: data/processed/clean_jobs.csv
Job skills saved to: data/processed/job_skills.csv

========== TRANSFORMATION SUMMARY ==========
Raw rows: 2528
Clean job rows after deduplication: 2516
Removed duplicate rows: 12
Job-skill rows: ...

========== CLEAN JOBS PREVIEW ==========
...

========== TOP 20 SKILLS ==========
SQL
Python
ETL
AWS
Spark
...

========== SALARY CHECK ==========
...

Transformation completed successfully.
```

The exact skill counts may vary depending on the skill dictionary.

---

## 11. Example Before and After

### Raw Input Example

| Field | Raw Value |
|---|---|
| Company Name | `Sagence\n4.5` |
| Salary Estimate | `$80K-$150K (Glassdoor est.)` |
| Location | `New York, NY` |
| Easy Apply | `-1` |
| Job Description | `Experience with Python, SQL, Airflow, Spark and AWS.` |

### Cleaned Output Example

| Field | Cleaned Value |
|---|---|
| clean_company_name | `Sagence` |
| salary_min | `80000` |
| salary_max | `150000` |
| salary_avg | `115000` |
| city | `New York` |
| state | `NY` |
| easy_apply_bool | `False` |
| num_skills | `5` |

### Job-Skill Output Example

| job_id | skill |
|---|---|
| 1 | Python |
| 1 | SQL |
| 1 | Airflow |
| 1 | Spark |
| 1 | AWS |

---

## 12. Why This Step Matters

The transform step prepares raw job posting data for analytics.

Raw data is difficult to analyze directly because:

- Salary is stored as text
- Company names may contain extra rating text
- Location is not split into city and state
- Job descriptions are unstructured text
- Skill requirements are hidden inside long descriptions
- Easy Apply values are inconsistent
- Duplicate rows may exist

The transform script solves these problems by creating clean and structured tables.

After this step, the data is ready for:

```text
PostgreSQL loading
SQL analytics
Data quality checks
Streamlit dashboard
Resume project demonstration
```

---

## 13. Data Engineering Concepts Demonstrated

This script demonstrates several core data engineering concepts:

### Data Cleaning

The script removes duplicates, cleans company names, and standardizes fields.

### Feature Engineering

The script creates new fields such as:

```text
salary_min
salary_max
salary_avg
city
state
easy_apply_bool
num_skills
```

### Text Parsing

The script extracts technical skills from unstructured job descriptions.

### Regex Processing

The script uses regular expressions to parse salary fields and match skill keywords.

### Data Normalization

The script separates job-level information from job-skill relationships.

### Pipeline Design

The script reads from a raw data layer and writes to a processed data layer.

---

## 14. Limitations

This transform script is designed as a first version.

Current limitations include:

1. Skill extraction is rule-based, so it only detects skills listed in `SKILL_KEYWORDS`.
2. Salary parsing currently focuses on salary values formatted with `K`, such as `$80K-$150K`.
3. Location parsing assumes most locations follow the `City, State` format.
4. The script does not yet create separate dimension tables for companies, locations, and skills.
5. The script does not yet validate all data quality issues automatically.
6. The script does not yet handle remote jobs as a separate location category.
7. Skill matching may miss variations such as `Amazon Web Services` if only `AWS` is listed.

These limitations can be improved in later versions.

---

## 15. Future Improvements

Possible future improvements include:

1. Expand the skill dictionary with more data engineering tools.
2. Add skill categories such as:
   - Programming Languages
   - Databases
   - Cloud Platforms
   - Big Data Tools
   - Orchestration Tools
   - BI Tools
3. Add a separate `skills.csv` dimension table.
4. Add a separate `companies.csv` dimension table.
5. Add a separate `locations.csv` dimension table.
6. Improve salary parsing for hourly wages or employer-provided salary ranges.
7. Add remote job detection.
8. Add automated data quality checks.
9. Add unit tests for transformation functions.
10. Move paths and skill keywords into a separate config file.
11. Add logging instead of using only print statements.
12. Add Airflow orchestration for scheduled pipeline runs.

---

## 16. Summary

The `src/transform.py` script converts raw Data Engineer job posting data into clean, structured, and analysis-ready files.

In simple terms:

```text
Raw DataEngineer.csv
        ↓
Clean and standardize fields
        ↓
Extract salary, location, and skills
        ↓
Generate clean_jobs.csv
        ↓
Generate job_skills.csv
        ↓
Ready for database loading and analytics
```

This script is a key part of the Job Market Analytics Data Pipeline because it turns messy job posting data into structured data that can support SQL analysis, dashboard visualization, and data engineering portfolio demonstration.
