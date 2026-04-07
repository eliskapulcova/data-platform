FROM apache/airflow:2.7.1-python3.10

# Install git as root
USER root
RUN apt-get update && apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Python packages
USER airflow
RUN pip install --no-cache-dir kafka-python confluent-kafka

# Install dbt and the Postgres adapter for the airflow user
RUN pip install --no-cache-dir dbt-core==1.9.3 dbt-postgres==1.9.1
