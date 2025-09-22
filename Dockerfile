FROM python:3.11-slim

ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
    apt-get install -y wget default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

RUN pip install pyspark==${SPARK_VERSION}
WORKDIR /app
COPY . .

CMD ["bash"]
