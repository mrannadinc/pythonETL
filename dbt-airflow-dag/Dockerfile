FROM quay.io/astronomer/astro-runtime:7.2.0

# install dbt into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m pip install --upgrade pip && \
    python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && pip install pytz && pip install dataset && pip install cosmos && pip install astronomer-cosmos && pip install dbt-core && pip install dbt-postgres && deactivate && \
    python -m pip install psycopg2

#RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#    deactivate

#RUN python -m venv dbt_venv &&  C:\Users\PC\Desktop\pythonETL\dbt_venv\Scripts\Activate.ps1 && \
#    pip install --no-cache-dir dbt-postgres && deactivate

#RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#    pip install --no-cache-dir dbt-snowflake && deactivate