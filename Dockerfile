FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless git \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Install Python dependencies (project requirements + runtime extras)
COPY requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project into the image and set working dir
COPY --chown=airflow:root . /opt/airflow/project
WORKDIR /opt/airflow/project

ENV PYTHONPATH=/opt/airflow/project
