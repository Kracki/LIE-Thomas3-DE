from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.scraper_async import scraper_async
from utils.cleaner_for_training import cleaner_for_training
from utils.cleaner_for_analysis import cleaner_for_analysis
from utils.market_analysis import market_analysis
from utils.train_regression_model import train_regression_model


default_args = {
    'retries': 1,
    'retry_delay': timedelta(hours=2)
}

with DAG(
        default_args=default_args,
        dag_id="model_dag",
        start_date=datetime(2023, 1, 1),
        schedule_interval="0 0 * * *"
) as dag:
    house_scraper_task = PythonOperator(
        task_id="house_scraper_task",
        python_callable=scraper_async,
        op_kwargs={"x": 1, "y": 2, "type": "house"}
    )
    apartment_scraper_task = PythonOperator(
        task_id="apartment_scraper_task",
        python_callable=scraper_async,
        op_kwargs={"x": 1, "y": 2, "type": "house"}
    )
    store_to_db_task = PostgresOperator(
        task_id="store_to_db_task",
        postgres_conn_id="immo_eliza",
        sql="""

            """,
    )
    data_cleaning_analysis_task = PythonOperator(
        task_id="data_cleaning_analysis_task",
        python_callable=cleaner_for_analysis
    )
    data_cleaning_training_task = PythonOperator(
        task_id="data_cleaning_training_task",
        python_callable=cleaner_for_training
    )
    store_analysis_task = PostgresOperator(
        task_id="store_analysis_task",
        postgres_conn_id="immo_eliza",
        sql="""

            """,
    )
    store_training_task = PostgresOperator(
        task_id="store_training_task",
        postgres_conn_id="immo_eliza",
        sql="""

            """,
    )
    market_analysis_task = PythonOperator(
        task_id="market_analysis_task",
        python_callable=market_analysis
    )
    train_model_task = PythonOperator(
        task_id="train_model_task",
        python_callable=train_regression_model
    )
    store_model_task = PostgresOperator(
        task_id="store_model_task",
        postgres_conn_id="immo_eliza",
        sql="""

            """,
    )
    # [house_scraper_task, apartment_scraper_task] >> store_to_db_task
    # store_to_db_task >> [data_cleaning_analysis_task, data_cleaning_training_task]
    # [data_cleaning_analysis_task, data_cleaning_training_task] >> [store_analysis_task, store_training_task]
    # [store_analysis_task, store_training_task] >> [market_analysis_task, train_model_task]
    # [market_analysis_task, train_model_task] >> store_model_task
    house_scraper_task >> store_to_db_task
    apartment_scraper_task >> store_to_db_task
    store_to_db_task >> data_cleaning_analysis_task >> store_analysis_task >> market_analysis_task
    store_to_db_task >> data_cleaning_training_task >> store_training_task >> train_model_task >> store_model_task
