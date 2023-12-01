from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'retries': 1,
    'retry_delay': timedelta(hours=2)
}


def seum(x, y):
    return x + y


with DAG(
    dag_id="empty",
    start_date=datetime(2023, 1, 1),
    default_args=default_args
) as dag:
    task1 = PythonOperator(
        task_id="test",
        python_callable=seum,
        op_kwargs={"x": 1, "y": 2}
    )

