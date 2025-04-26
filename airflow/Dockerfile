FROM apache/airflow:latest-python3.11

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev procps curl build-essential netcat-openbsd \
        openjdk-17-jdk libffi-dev libssl-dev libpq-dev cmake libclang-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cài Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Thiết lập JAVA_HOME
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export PATH=$PATH:$JAVA_HOME/bin" >> /etc/environment

# Thêm vào cuối Dockerfile của Airflow
RUN mkdir -p /opt/bitnami/spark/jars && \
    cd /opt/bitnami/spark/jars && \
    curl -O https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.4/gcs-connector-hadoop3-2.2.4-shaded.jar && \
    curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

# Đảm bảo user airflow có quyền truy cập thư mục
RUN chown -R airflow:root /opt/bitnami

USER airflow

ENV PATH="/home/airflow/.cargo/bin:${PATH}"

RUN pip install --upgrade pip && \
    pip install maturin && \
    pip install apache-airflow-providers-apache-spark pyspark==3.4.1 underthesea numpy 'elasticsearch>=8.0.0'
