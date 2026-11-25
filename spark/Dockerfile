# Imagen única para todo el lado "Big Data": ingesta streaming, mantenimiento Iceberg y dbt.
# Las mismas jars que en AWS Glue 4.0/5.0 (Spark 3.5 + Iceberg), para que los jobs sean portables.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.6.1
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG MAVEN=https://repo1.maven.org/maven2

# Iceberg (runtime + bundle AWS para S3FileIO) y conector Kafka de Structured Streaming
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

# dbt con el adaptador Spark en modo "session" (usa PySpark local; sin Thrift Server permanente)
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir "dbt-core==1.11.*" "dbt-spark==1.11.*"

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PATH="${PATH}:/opt/spark/bin"

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/log4j2.properties /opt/spark/conf/log4j2.properties

RUN mkdir -p /opt/checkpoints /opt/jobs /opt/dbt
WORKDIR /opt/spark/work-dir
