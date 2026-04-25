# Job Market Analytics Data Pipeline

An end-to-end **Data Engineering** project that transforms raw Data Engineer job postings into clean analytics tables, validates data quality, loads the data into **PostgreSQL**, and visualizes job-market insights through an interactive **Streamlit dashboard**.

---

## Project Overview

This project analyzes Data Engineer job postings to answer questions such as:

- Which technical skills are most demanded?
- Which cities have the most Data Engineer jobs?
- Which skills are associated with higher average salaries?
- What does the salary distribution look like?
- Which companies are hiring the most Data Engineers?

The project follows a standard data engineering workflow:

```text
Raw CSV
   ↓
Extract
   ↓
Transform
   ↓
Data Quality Check
   ↓
PostgreSQL Load
   ↓
SQL Analytics
   ↓
Streamlit Dashboard
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Programming | Python |
| Data Processing | Pandas, Regex |
| Database | PostgreSQL |
| Database Connection | SQLAlchemy, psycopg2 |
| Analytics | SQL |
| Dashboard | Streamlit, Plotly |
| Data Quality | Custom Python validation checks |

---

## Dataset

The raw dataset contains **2,528 Data Engineer job postings**.

Example raw fields include:

- Job Title
- Salary Estimate
- Job Description
- Rating
- Company Name
- Location
- Industry
- Sector
- Revenue
- Easy Apply

After transformation, the project generates cleaned and structured tables for analysis.

---

## Project Structure

```text
job-market-analytics-data-engineer/
│
├── data/
│   ├── raw/
│   │   └── DataEngineer.csv
│   └── processed/
│       ├── raw_preview.csv
│       ├── clean_jobs.csv
│       ├── job_skills.csv
│       └── data_quality_report.txt
│
├── dashboard/
│   └── app.py
│
├── docs/
│   └── transform_readme.md
│
├── sql/
│   ├── create_tables.sql
│   └── analysis_queries.sql
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── data_quality.py
│   └── load.py
│
├── requirements.txt
└── README.md
```

---

## Pipeline Steps

### 1. Extract

`src/extract.py` reads the raw CSV file and prints basic dataset information.

It checks:

- Number of rows and columns
- Column names
- Missing values
- Duplicate rows
- Data types

It also saves a small preview file:

```text
data/processed/raw_preview.csv
```

---

### 2. Transform

`src/transform.py` cleans and restructures the raw job posting data.

Main transformations include:

- Removing duplicate rows
- Cleaning company names
- Parsing salary ranges into numeric fields
- Splitting location into city and state
- Normalizing the Easy Apply field
- Extracting technical skills from job descriptions
- Creating a normalized job-skill mapping table

Output files:

```text
data/processed/clean_jobs.csv
data/processed/job_skills.csv
```

---

### 3. Data Quality Validation

`src/data_quality.py` validates the cleaned outputs.

Checks include:

- Missing values
- Duplicate job IDs
- Duplicate job-skill pairs
- Invalid salary ranges
- Salary outliers
- Empty job titles or company names
- Skill extraction coverage
- Referential integrity between `clean_jobs` and `job_skills`

Example results:

```text
Clean job rows: 2,516
Job-skill rows: 13,486
Skill coverage rate: 84.50%
Duplicate job_id count: 0
Invalid salary ranges: 0
```

The report is saved to:

```text
data/processed/data_quality_report.txt
```

---

### 4. PostgreSQL Loading

`sql/create_tables.sql` creates two PostgreSQL tables:

#### `clean_jobs`

Stores job-level information such as:

- job title
- company
- city and state
- salary range
- industry and sector
- number of extracted skills

#### `job_skills`

Stores one row per job-skill pair.

Example:

| job_id | skill |
|---|---|
| 1 | Python |
| 1 | SQL |
| 1 | Airflow |

`src/load.py` loads the processed CSV files into PostgreSQL and validates row counts after loading.

---

### 5. SQL Analytics

`sql/analysis_queries.sql` contains SQL queries for job-market analysis.

Examples include:

- Top 20 most demanded skills
- Top cities by job count
- Average salary by skill
- Average salary by city
- Salary distribution buckets
- Top hiring companies
- Easy Apply percentage

---

### 6. Streamlit Dashboard

`dashboard/app.py` connects directly to PostgreSQL and visualizes analytics results.

Dashboard sections include:

- Overview KPI cards
- Top required skills
- Top cities by job count
- Average salary by skill
- Salary distribution
- Top hiring companies
- Easy Apply distribution
- Cleaned job data preview

---

## How to Run the Project

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Or install manually:

```bash
pip install pandas sqlalchemy psycopg2-binary streamlit plotly
```

---

### 2. Run extraction

```bash
python3 src/extract.py
```

---

### 3. Run transformation

```bash
python3 src/transform.py
```

---

### 4. Run data quality checks

```bash
python3 src/data_quality.py
```

---

### 5. Create PostgreSQL database

```bash
createdb job_market_db
```

Then create tables:

```bash
psql job_market_db -f sql/create_tables.sql
```

---

### 6. Load data into PostgreSQL

```bash
python3 src/load.py
```

---

### 7. Run SQL analysis

```bash
psql job_market_db -f sql/analysis_queries.sql
```

---

### 8. Launch dashboard

```bash
streamlit run dashboard/app.py
```

Then open the local Streamlit URL shown in the terminal.

---

## Key Insights Generated

The pipeline can generate insights such as:

- **Python**, **SQL**, **AWS**, **Spark**, and **Java** are among the most frequently requested skills.
- Skill extraction reached approximately **84.5% coverage** across cleaned job postings.
- Average salary can be analyzed by skill, city, company, and industry.
- PostgreSQL enables repeatable and scalable analytics queries over the cleaned dataset.

---

## Data Engineering Concepts Demonstrated

This project demonstrates:

- ETL pipeline design
- Data cleaning and standardization
- Regex-based parsing
- Unstructured text feature extraction
- Normalized data modeling
- PostgreSQL schema design
- SQL analytics
- Data quality validation
- Dashboard development
- End-to-end pipeline documentation

---

## Future Improvements

Potential next steps include:

- Add Airflow orchestration for scheduled pipeline runs
- Containerize the project with Docker
- Add dbt models for analytics transformations
- Expand the skill dictionary and add skill categories
- Add unit tests for transformation functions
- Add remote job detection
- Deploy the dashboard to a cloud server


