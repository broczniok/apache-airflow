from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.task_group import TaskGroup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def create_section():
    """
    Create tasks in the outer section.
    """
    dummies = [EmptyOperator(task_id=f'task-{i + 1}') for i in range(5)]
    logger.info("Created outer section tasks: %s", dummies)

    with TaskGroup("inside_section_1") as inside_section_1:
        inside_1_tasks = [EmptyOperator(task_id=f'task-{i + 1}') for i in range(3)]
        logger.info("Created inside_section_1 tasks: %s", inside_1_tasks)

    dummies[-1] >> inside_section_1


def tasks_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False
    )

    subtask_1 = EmptyOperator(task_id='subtask_1', queue='jobs_dag', dag=subdag)
    subtask_2 = EmptyOperator(task_id='subtask_2', queue='jobs_dag', dag=subdag)
    subtask_3 = EmptyOperator(task_id='subtask_3', queue='jobs_dag', dag=subdag)

    logger.info("Created subdag tasks: %s, %s, %s", subtask_1, subtask_2, subtask_3)

    subtask_1 >> subtask_2 >> subtask_3

    return subdag


with DAG(dag_id="task_group_dag", start_date=days_ago(1), schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for Section 1") as section_1:
        create_section()

    sub_dag_task = SubDagOperator(
        task_id='sub_dag',
        subdag=tasks_sub_dag(
            parent_dag_name='task_group_dag',
            child_dag_name='sub_dag',
            start_date=days_ago(1),
            schedule_interval='@daily'
        ),
        queue='jobs_dag'
    )

    some_other_task = EmptyOperator(task_id="some-other-task")

    end = EmptyOperator(task_id='end')

    start >> section_1 >> some_other_task >> sub_dag_task >> end
    logger.info("Main DAG tasks created: start, section_1, some_other_task, sub_dag_task, end")
