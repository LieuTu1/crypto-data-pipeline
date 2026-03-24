FROM apache/airflow:2.8.2
# setup things
USER root
# setup pkgs
RUN apt-get update && apt-get install -y wget gnupg openjdk-17-jdk

RUN mkdir /content
# mongosh
RUN wget -qO "/content/mongodb-mongosh_amd64.deb" "https://downloads.mongodb.com/compass/mongodb-mongosh_1.9.1_amd64.deb"
RUN dpkg -i "/content/mongodb-mongosh_amd64.deb"

# mongodb-database-tools
RUN wget -qO "/content/mongodb-database-tools.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian11-x86_64-100.7.1.deb"
RUN dpkg -i "/content/mongodb-database-tools.deb"

# JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# setup things
USER airflow

# Use Airflow constraints to avoid dependency conflicts for Airflow extras.
ARG AIRFLOW_VERSION=2.8.2
# Match the Python version bundled with apache/airflow:2.8.2 (3.8)
ARG PYTHON_VERSION=3.8
ENV CONSTRAINTS_FILE="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --upgrade \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-mysql \
    PyMySQL \
    pandas \
    pyspark \
    findspark \
    gdown \
    --constraint "${CONSTRAINTS_FILE}" --user

# Install ML deps separately (outside Airflow constraints) and force CPU wheels for torch.
RUN pip install --no-cache-dir --upgrade \
    "torch>=2.1.0" \
    "transformers>=4.36.0" \
    --extra-index-url https://download.pytorch.org/whl/cpu \
    --user

# REMEMBER TO BUILD ME
# docker build -t my_airflow .
