from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_data_pipeline',
    default_args=default_args,
    description='Daily run of generate_data, data_populating, data_quality_standards, monitoring_audit',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python /home/timobank/airflow/src/generate_data.py'
    )
    data_quality_first = BashOperator(
        task_id='data_quality_standards_first_run',
        bash_command='python /home/timobank/airflow/src/data_quality_standards.py'
    )

    data_populating = BashOperator(
        task_id='data_populating',
        bash_command='python /home/timobank/airflow/src/data_populating.py'
    )

    data_quality_second = BashOperator(
        task_id='data_quality_standards_second_run',
        bash_command='python /home/timobank/airflow/src/data_quality_standards.py'
    )

    monitoring_audit = BashOperator(
        task_id='monitoring_audit',
        bash_command='python /home/timobank/airflow/src/monitoring_audit.py'
    )

    generate_data >> data_quality_first >> data_populating >> data_quality_second >> monitoring_audit