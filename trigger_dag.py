from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
from datetime import datetime
import pytz

EXECUTION_DATE = datetime.now(pytz.utc)


def get_time(context):
    ti = context['task_instance']
    start_time = ti.start_date
    print("start time:", start_time)
    ti.xcom_push(key='start_time', value=start_time.isoformat())
    result = ti.xcom_pull(key='start_time', task_ids='Trigger_DAG')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='trigger_dag',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 6, 29),
        catchup=False
) as dag:
    path = Variable.get('run_path', default_var='run2')

    task_1 = FileSensor(
        task_id='sensor_wait_run_file',
        filepath=path,
        fs_conn_id='fs_default',
        queue='jobs_dag'
    )

    task_2 = TriggerDagRunOperator(
        task_id='Trigger_DAG',
        trigger_dag_id='process_results',
        on_success_callback=get_time,
        poke_interval=60,
        queue='jobs_dag'
    )


    task_1 >> task_2
