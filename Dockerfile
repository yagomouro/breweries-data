FROM apache/airflow:2.10.2-python3.11

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk ant wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/usr/local/spark

RUN wget -qO- "http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | \
    tar -xz -C /usr/local && \
    mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm -rf /tmp/*

ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow