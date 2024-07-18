from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG, task
from datetime import datetime, timedelta
import logging
from trigger_dag import EXECUTION_DATE

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def get_time(_,**kwargs):
    ti = kwargs['ti']
    time = ti.xcom_pull(dag_id='trigger_dag', key='start_time', include_prior_dates=True)
    logging.info(f"Retrieved start_time from XCom: {time}")
    return datetime.fromisoformat(time) if time else None


def tasks_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    @subdag.task(task_id='print_results_task', queue='jobs_dag', dag=subdag, multiple_outputs=True)
    def print_results(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(key='push_message', dag_id='dag_id_1', task_ids='Trigger_DAG', include_prior_dates=True)
        print(f"Result from triggered DAG: {result}")
        print(f"Task context: {kwargs}")
        #return {"result": result, "context": kwargs}

    dag_sensor_task = ExternalTaskSensor(
        task_id='sensor_triggered_dag',
        external_dag_id='trigger_dag',
        external_task_id='Trigger_DAG',
        execution_date_fn=get_time,
        mode='poke',
        queue='jobs_dag',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        poke_interval=60,
        timeout=600,
        dag=subdag
    )

    rm_run_task = BashOperator(
        task_id='remove_run_file',
        bash_command='rm /opt/airflow/run2',
        queue='jobs_dag',
        dag=subdag
    )

    timestamp_task = BashOperator(
        task_id='create_finished_timestamp',
        bash_command='touch /opt/airflow/file_{{ ts_nodash }}',
        queue='jobs_dag',
        dag=subdag
    )

    dag_sensor_task >> print_results() >> rm_run_task >> timestamp_task

    return subdag

with DAG(
        dag_id='process_results',
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 6, 29),
        catchup=False
) as dag:

    task_1 = EmptyOperator(task_id='task1')

    sub_dag_task = SubDagOperator(
        task_id='sub_dag',
        subdag=tasks_sub_dag(
            parent_dag_name='process_results',
            child_dag_name='sub_dag',
            start_date=dag.start_date,
            schedule_interval=dag.schedule_interval
        ),
        queue='jobs_dag',
        dag=dag
    )

    task_1 >> sub_dag_task
