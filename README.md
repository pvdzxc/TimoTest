==============================
💼 Banking Data Assignment
==============================

Project Overview
----------------
This project sets up a secure and compliant data platform for a simplified banking system.
It uses Docker to orchestrate PostgreSQL and Apache Airflow for data pipeline execution.

----------------
🚀 Getting Started
----------------

1. Open a terminal in the project root directory.
2. Run the following command:

   docker compose up

This will start all required services using Docker Compose.

----------------
🔗 Service Access
----------------

🌬️ Apache Airflow
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

To run the daily data pipeline:
1. Visit http://localhost:8080
2. Go to the "DAGs" tab
3. Select 'daily-data-pipeline'
4. Click "Trigger DAG"

🐘 pgAdmin (PostgreSQL UI)
- URL: http://localhost:80
- Username: admin@example.com
- Password: admin

📦 PostgreSQL Database Connection
- Host: postgres
- Database: airflow
- Username: airflow
- Password: airflow

---------------------
🧱 Project Structure
---------------------

project-root/
│
├── docker-compose.yaml         # Defines services (Airflow, PostgreSQL, pgAdmin)
├── dags/                       # Airflow DAGs (pipelines)
├── sql/                        # SQL scripts for schema and sample data
├── logs/                       # Airflow logs
└── README.txt                  # This file

--------------------------
🛠️ Technologies Used
--------------------------

- Python 3
- Apache Airflow
- PostgreSQL
- pgAdmin
- Docker & Docker Compose

--------------------------
📬 Support
--------------------------

For questions or issues, feel free to reach out to the project maintainer.
