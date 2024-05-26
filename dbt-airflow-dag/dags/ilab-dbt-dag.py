import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    dag_id="ilab_dbt",
    start_date=datetime(2022, 11, 27),
    schedule="@daily",
) as dag:
    
    start = EmptyOperator(task_id="ingestion_workflow")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='dbt run',
        dag=dag
    )

    end = EmptyOperator(task_id="some_extraction")

    start >> dbt_run >> end