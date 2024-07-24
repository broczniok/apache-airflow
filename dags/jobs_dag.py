import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
import psycopg2
from psycopg2 import sql
import configuration as C
from PostgreSQLCountRows import PostgreSQLCountRows

POSTGRES_CONN_ID = 'my_postgres_conn'


def start_processing(dag_id, table_name):
    print(f"{dag_id} start processing tables in database: {table_name}")


def check_table_existence(table_name, **kwargs):
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password=C.password,
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s"), [table_name])
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return 'insert_rowv2' if exists else 'create_tablev2'
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return 'create_tablev2'


def push_message(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='push_message', value=str(ti.run_id))


config = {
    'dag_id_1': {'schedule_interval': "@daily", "start_date": datetime(2024, 6, 29), 'table_name': "table_name_1"},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2024, 7, 2), 'table_name': "table_name_2"},
    'dag_id_3': {'schedule_interval': "@hourly", "start_date": datetime(2024, 7, 3), 'table_name': "table_name_3"}
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

for conf_name, conf in config.items():
    with DAG(dag_id=conf_name,
             default_args=default_args,
             schedule_interval=conf['schedule_interval'],
             start_date=conf['start_date'],
             catchup=False) as dag:

        print_process_start = PythonOperator(task_id='print_process_start', python_callable=start_processing, op_kwargs={'dag_id': conf_name, 'table_name': conf['table_name']}, queue='jobs_dag', )
        task_branch = BranchPythonOperator(task_id='check_table_exists', python_callable=check_table_existence, op_kwargs={'table_name': conf['table_name']}, provide_context=True, queue='jobs_dag', )
        insert_row = SQLExecuteQueryOperator(
            task_id='insert_rowv2',
            sql="""
                INSERT INTO table_name VALUES
                (%(item1)s, '{{ ti.xcom_pull(task_ids='get_current_user') }}', %(item3)s);
              """,
            conn_id=POSTGRES_CONN_ID,
            trigger_rule=TriggerRule.NONE_FAILED,
            parameters={
                "item1": uuid.uuid4().int % 123456789,  # Use Jinja to generate UUID
                "item3": str(datetime.now())
            },
            queue='jobs_dag'
        )
        push_message = PythonOperator(task_id='push_message', python_callable=push_message, trigger_rule=TriggerRule.NONE_FAILED, queue='jobs_dag' )
        create_table = SQLExecuteQueryOperator(task_id='create_tablev2', sql="""
            CREATE TABLE table_name(custom_id integer NOT NULL, 
   user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL); 
          """, trigger_rule=TriggerRule.ONE_FAILED, conn_id=POSTGRES_CONN_ID, queue='jobs_dag')
        get_current_user = BashOperator(task_id='get_current_user', bash_command='whoami', do_xcom_push=True, queue='jobs_dag')
        query_table = PostgreSQLCountRows(
            task_id='query_table',
            table_name='table_name',
            conn_id='my_postgres_conn',
            do_xcom_push=True,
            queue='jobs_dag'
        )
        print_process_start >> get_current_user >> task_branch >> insert_row >> push_message >> query_table
        task_branch >> create_table >> insert_row


        globals()[conf_name] = dag
