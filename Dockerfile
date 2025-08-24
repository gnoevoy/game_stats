FROM astrocrpublic.azurecr.io/runtime:3.0-6

# Create seperate virtual environment for dbt
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core dbt-bigquery && deactivate
