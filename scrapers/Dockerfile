ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}
RUN apt-get update && apt-get install -y --no-install-recommends freetds-bin krb5-user ldap-utils libffi7 libsasl2-2 libsasl2-modules libssl1.1 locales lsb-release sasl2-bin sqlite3 unixodbc firefox-esr

COPY install_prerequirements.sh .
RUN sh install_prerequirements.sh
COPY scrapers .

RUN mkdir raw_data log_dir
ENV RAW_FILES_DIR="/mair/raw_data" \
    SCRAPER_LOGS_DIR="/mair/log_dir" \
    GECKODRIVER_PATH="/mair/geckodriver" \
    LOGGING_LEVEL="INFO"

# Just to init airflow directory
RUN airflow version

RUN mkdir -p /root/airflow/dags && mv *.dag.py /root/airflow/dags/
