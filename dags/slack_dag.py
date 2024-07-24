from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


slack_token = Variable.get("slack_token2")
client = WebClient(token=slack_token)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


def slack_message():
    try:
        response = client.chat_postMessage(
            channel="general",
            text="Hello from your app! :tada:")
        print("success")
    except SlackApiError as e:
        print(f"Error posting message: {e.response['error']}")
        raise


with DAG(dag_id='slack_dag',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2024, 7, 2),
         catchup=False) as dag:
    task_1 = PythonOperator(task_id='slack_message', python_callable=slack_message, dag=dag)

    globals()['slack_dag'] = dag