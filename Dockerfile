FROM apache/airflow:2.10.4-python3.11

USER root

# Install OpenJDK-17
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# RUN echo JAVA_HOME=$([[ -d /usr/lib/jvm/java-17-openjdk-amd64 ]] && echo /usr/lib/jvm/java-17-openjdk-amd64 || [[ -d /usr/lib/jvm/java-17-openjdk-arm64 ]] && echo /usr/lib/jvm/java-17-openjdk-arm64 || echo "") >> /etc/environment

USER airflow

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# USER airflow
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt