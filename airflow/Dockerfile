FROM apache/airflow:2.11.0

# Upgrade pip first
RUN pip install --upgrade pip

# Install HDFS Python client
RUN pip install "hdfs==2.7.3"

# Install Airflow HDFS provider (without optional kerberos to avoid build issues)
RUN pip install --no-deps apache-airflow-providers-apache-hdfs