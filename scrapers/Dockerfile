ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY scrapers .

# Just to init airflow directory
RUN airflow version

RUN mkdir -p /root/airflow/dags && mv *.dag.py /root/airflow/dags/
