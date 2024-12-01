FROM quay.io/astronomer/astro-runtime:12.4.0
# install dbt into a venv to avoid package dependency conflicts
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate