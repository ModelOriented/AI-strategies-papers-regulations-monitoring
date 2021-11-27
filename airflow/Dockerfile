ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

RUN mkdir -p /root/airflow/dags

COPY scripts scripts
COPY airflow/*.dag.py /root/airflow/dags/ 

# Just to init airflow directory
RUN airflow version
