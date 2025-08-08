--------------------------
 Banking Data Assignment
--------------------------

Project Overview
----------------
This project sets up a secure and compliant data platform for a simplified banking system.
It uses Docker to orchestrate, PostgreSQL as database engine and Apache Airflow for data pipeline execution.

----------------
 One-Command setup
----------------

1. Open a terminal in the project root directory.
2. Run the following command:

   docker compose up

This will start all required services using Docker Compose.

----------------
 Service Access
----------------

🌬 Apache Airflow
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

To run the daily data pipeline:
1. Visit http://localhost:8080
2. Go to the "DAGs" tab
3. Select 'daily-data-pipeline'
4. Click "Trigger DAG"
   * The DAG are automatically executed daily

🐘 pgAdmin (PostgreSQL UI)
- URL: http://localhost:80
- Username: admin@example.com
- Password: admin

Check the data storing after each DAG trigger by select schema of: customer, tabtransaction, bankaccount, device, authenticationlog

📦 PostgreSQL Database Connection
- Host: postgres
- Database: airflow
- Username: airflow
- Password: airflow

Progress to PostgreSQL database by using pgAdmin "add connection" with above information

---------------------
Project Structure
---------------------

project-root/

│

├── docker-compose.yaml         # Defines services (Airflow, PostgreSQL, pgAdmin)

├── dags/                       # Airflow DAGs (pipelines)

├── data/                       # Data source folder 

├── logs/                       # DAG processor, data quality, risk audit logging

├── sql/                        # SQL scripts for SQL schema/table

├── src(scripts)/               # Python executing file, python library requirement 

└── README.txt                  # This file

--------------------------
 Technologies Used
--------------------------

- Python 3.11
- Apache Airflow
- PostgreSQL
- Docker Compose

--------------------------
 Scripts Utility
--------------------------

The Data is created by the generate_data.py, virtualizing data daily stream of banking activity,

After the first stage of data quality checking by data_quality_standards.py, the data is inserted to SQL schemas through data_populating.py,

Then, the second stage of data quality checking is performed by the same python script, 

Finally monitoring_audit.py script will check any fraud/risk data insight and alert via logging or database risk tagging.

--------------------------
 Support
--------------------------

For questions or issues, feel free to reach out to the project maintainer via pvdzxc@gmail.com.
