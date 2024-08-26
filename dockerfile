FROM apache/airflow:latest

USER root
RUN apt-get update && \
  apt-get -y install git
RUN mkdir -p /opt/airflow
RUN chmod 777 /opt/airflow
USER airflow
EXPOSE 8082
