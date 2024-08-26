FROM apache/airflow:latest

USER root
RUN apt-get update && \
  apt-get -y install git && \
  apt-get install -y virtualenv
RUN mkdir -p /opt/airflow
RUN chmod 777 /opt/airflow
RUN cd /opt/airflow/ && virtualenv .venv && source .venv/bin/activate
COPY ./requirements.txt ./requirements.txt
USER airflow
RUN pip install -r requirements.txt
EXPOSE 8082
