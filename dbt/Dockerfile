FROM python:3.12-slim

WORKDIR /script

RUN pip install --no-cache-dir dbt-core dbt-bigquery

COPY . .

CMD [ "dbt", "build"]
