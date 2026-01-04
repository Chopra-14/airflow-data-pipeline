# 🚀 Airflow Data Processing Workflows with Docker

## 📌 Project Overview
This project demonstrates a complete, production-style **data engineering workflow** using **Apache Airflow** and **Docker**.  
It showcases how to orchestrate ETL pipelines, apply data transformations, handle conditional logic, manage failures, and validate workflows using unit tests.

The project consists of **five independent DAGs**, each representing a real-world data engineering scenario.

---

## 🛠️ Tech Stack
- **Apache Airflow 2.8.0**
- **Docker & Docker Compose**
- **PostgreSQL**
- **Python**
- **Pandas**
- **PyArrow (Parquet)**
- **Pytest**

---

## 📂 Project Structure
airflow-data-pipeline/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── dags/
│ ├── dag1_csv_to_postgres.py
│ ├── dag2_data_transformation.py
│ ├── dag3_postgres_to_parquet.py
│ ├── dag4_conditional_workflow.py
│ └── dag5_notification_workflow.py
├── tests/
│ ├── test_dag1.py
│ ├── test_dag2.py
│ └── test_utils.py
├── data/
│ └── input.csv
├── output/
├── logs/
└── plugins/

yaml
Copy code

---

## ⚙️ Setup Instructions

### 1️⃣ Prerequisites
Ensure the following are installed:
- Docker Desktop
- Git Bash / Terminal

---

### 2️⃣ Start the Airflow Environment
From the project root directory:

```bash
docker compose up -d
Access the Airflow UI:

arduino
Copy code
http://localhost:8080
Login credentials:

pgsql
Copy code
Username: admin
Password: admin
📊 DAG Descriptions
🔹 DAG 1: CSV to PostgreSQL Ingestion
DAG ID: csv_to_postgres_ingestion

Schedule: Daily

Reads employee data from a CSV file

Loads data into PostgreSQL

Ensures idempotency by truncating the table before each load

🔹 DAG 2: Data Transformation Pipeline
DAG ID: data_transformation_pipeline

Schedule: Daily

Reads data from PostgreSQL

Applies transformations:

full_info (name + city)

age_group (Young / Mid / Senior)

salary_category (Low / Medium / High)

year_joined

Stores transformed data in a new table

🔹 DAG 3: PostgreSQL to Parquet Export
DAG ID: postgres_to_parquet_export

Schedule: Weekly

Exports transformed data to Parquet format

Uses PyArrow with Snappy compression

Validates Parquet file integrity

🔹 DAG 4: Conditional Workflow
DAG ID: conditional_workflow_pipeline

Schedule: Daily

Uses BranchPythonOperator

Executes different branches based on the day of the week:

Weekday

End-of-week

Weekend

Demonstrates conditional execution and trigger rules

🔹 DAG 5: Notification Workflow
DAG ID: notification_workflow

Schedule: Daily

Simulates a risky operation

Demonstrates:

Success and failure callbacks

Trigger rules (all_success, all_failed, all_done)

Cleanup task that always runs

🧪 Unit Testing
All DAGs are validated using pytest without executing Airflow tasks.

Run Tests:
bash
Copy code
docker compose exec airflow-webserver pytest /opt/airflow/tests -v
Tests Validate:
DAGs load without import errors

Correct number of tasks

Proper task dependencies

No cyclic dependencies

Unique DAG IDs

🧠 Key Concepts Demonstrated
ETL orchestration

Idempotent data pipelines

Data transformation logic

Conditional workflows

Failure handling & callbacks

Parquet-based analytics storage

Containerized data engineering

DAG validation using unit tests

