FROM apache/airflow:3.3.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt