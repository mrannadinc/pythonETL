import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres", 
        profile_args={"database": "postgres", "schema": "postgres"},
    )
)

dbt_postgres_dag = DbtDag(
    project_config=ProjectConfig("dags/dbt/ilab",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    start_date=datetime(2024, 5, 26),
    schedule="@daily",
    dag_id='ilab_dbt'
)

dbt_postgres_dag