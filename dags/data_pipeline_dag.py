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
    schedule ='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
<<<<<<< HEAD
        bash_command='python /opt/airflow/scripts/generate_data.py'
    )
    data_quality_first = BashOperator(
        task_id='data_quality_standards_first_run',
        bash_command='python /opt/airflow/scripts/data_quality_standards.py'
=======
        bash_command='python ~/TimoTest/src/generate_data.py'
    )
    data_quality_first = BashOperator(
        task_id='data_quality_standards_first_run',
        bash_command='python ~/TimoTest/src/data_quality_standards.py'
>>>>>>> a871091e71e674c1dabafd8277490e72dc4aa4ed
    )

    data_populating = BashOperator(
        task_id='data_populating',
<<<<<<< HEAD
        bash_command='python /opt/airflow/scripts/data_populating.py'
=======
        bash_command='python ~/TimoTest/src/data_populating.py'
>>>>>>> a871091e71e674c1dabafd8277490e72dc4aa4ed
    )

    data_quality_second = BashOperator(
        task_id='data_quality_standards_second_run',
<<<<<<< HEAD
        bash_command='python /opt/airflow/scripts/data_quality_standards.py'
=======
        bash_command='python ~/TimoTest/src/data_quality_standards.py'
>>>>>>> a871091e71e674c1dabafd8277490e72dc4aa4ed
    )

    monitoring_audit = BashOperator(
        task_id='monitoring_audit',
<<<<<<< HEAD
        bash_command='python /opt/airflow/scripts/monitoring_audit.py'
=======
        bash_command='python ~/TimoTest/src/monitoring_audit.py'
>>>>>>> a871091e71e674c1dabafd8277490e72dc4aa4ed
    )

    generate_data >> data_quality_first >> data_populating >> data_quality_second >> monitoring_audit