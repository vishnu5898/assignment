FROM apache/airflow:latest

USER root
RUN apt-get update && \
  apt-get -y install git
USER airflow
RUN mkdir -p /opt/airflow
EXPOSE 8082
