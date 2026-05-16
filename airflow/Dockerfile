FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gcc python3-dev openjdk-17-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt

# Install PySpark with no hash check (fixes corrupted download retry)
RUN pip install --no-cache-dir --timeout=600 --retries=10 --no-deps pyspark==3.5.3 py4j==0.10.9.7 delta-spark==3.2.1

# Install remaining packages
RUN pip install --no-cache-dir --timeout=120 --retries=3 -r /tmp/requirements.txt
