import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

PATH_TO_DBT_PROJECT = "/usr/local/airflow/dags/dbt/dbt_ilab"
PATH_TO_DBT_VENV = "/usr/local/airflow/dbt_venv/bin/activate"

with DAG(
    dag_id="ilab_dbt",
    start_date=datetime(2024, 5, 27),
    schedule="@daily",
) as dag:
    
    start = EmptyOperator(task_id="start")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="source {0} && dbt run".format(PATH_TO_DBT_VENV),
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
        dag=dag
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_run >> end