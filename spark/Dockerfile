FROM bitnami/spark:3.4.1

USER root

# Cài đặt các dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python cần thiết
RUN pip3 install --no-cache-dir \
    underthesea \
    numpy \
    elasticsearch>=8.0.0 \
    pyspark>=3.5.0 \
    kafka-python

# Tạo thư mục cho Google Cloud credentials
RUN mkdir -p /app
COPY job/key.json /app/key.json

# Tải các connector JAR files
RUN cd /opt/bitnami/spark/jars && \
    curl -O https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.4/gcs-connector-hadoop3-2.2.4-shaded.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.0/spark-streaming-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -O https://repo1.maven.org/maven2/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar && \
    curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.0/elasticsearch-spark-30_2.12-8.11.0.jar

RUN mkdir -p /tmp/checkpoint && \
    chown -R 1001:1001 /tmp/checkpoint && \
    chmod -R 755 /tmp/checkpoint
# Copy job files
COPY job /opt/bitnami/spark/job

USER 1001
