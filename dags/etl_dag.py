from airflow import DAG
from datetime import datetime
from smart_file_sensor import SmartFileSensor
from airflow.operators.python import PythonOperator
import csv
from collections import defaultdict

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def count_numbers(**kwargs):
    encodings_to_try = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252']
    year_count = defaultdict(int)
    ti = kwargs['ti']
    success = False

    for encoding in encodings_to_try:
        try:
            with open('run/monroe-county-crash.csv', 'r', encoding=encoding) as csvfile:
                reader = csv.DictReader(csvfile)

                headers = reader.fieldnames
                if 'Year' not in headers or 'Master Record Number' not in headers:
                    raise ValueError('CSV must contain "Year" and "Master Record Number" columns')

                for row in reader:
                    year = row['Year']
                    master_record_number = row['Master Record Number']
                    if master_record_number:
                        year_count[year] += 1

                success = True
                break  # Exit the loop if successful
        except UnicodeDecodeError as e:
            print(f"Failed to decode with encoding: {encoding}. Error: {e}")

    if not success:
        raise ValueError("Failed to read the CSV file with any of the tried encodings.")

    ti.xcom_push(key='year_count', value=dict(year_count))


def print_numbers(**kwargs):
    ti = kwargs['ti']
    year_counts = ti.xcom_pull(dag_id='etl_dag', key='year_count', include_prior_dates=True)
    for year, count in sorted(year_counts.items()):
        print(f"Year: {year} - Count: {count}")


with DAG(
        dag_id='etl_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 6, 29),
        catchup=False
) as dag:
    file = "monroe-county-crash.csv"
    task_1 = SmartFileSensor(
        task_id='sensor_wait_run_file',
        filepath=file,
        fs_conn_id='fs_default',
        queue='jobs_dag'
    )

    task_2 = PythonOperator(task_id='process_file', python_callable=count_numbers, queue='jobs_dag', dag=dag)

    task_3 = PythonOperator(task_id='show_results', python_callable=print_numbers, queue='jobs_dag', dag=dag)

    task_1 >> task_2 >> task_3

