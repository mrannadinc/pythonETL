FROM quay.io/astronomer/astro-runtime:11.3.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install dbt-postgres --no-cache-dir && deactivate

#RUN python -m venv dbt_venv &&  C:\Users\PC\Desktop\pythonETL\dbt_venv\Scripts\Activate.ps1 && \
#    pip install --no-cache-dir dbt-postgres && deactivate