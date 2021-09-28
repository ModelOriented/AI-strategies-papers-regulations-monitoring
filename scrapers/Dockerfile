FROM piotrp/mair_common
RUN apt-get update && apt-get install -y --no-install-recommends freetds-bin krb5-user ldap-utils libffi7 libsasl2-2 libsasl2-modules libssl1.1 locales lsb-release sasl2-bin sqlite3 unixodbc firefox-esr
RUN wget -N https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz && tar xzvf geckodriver-v0.30.0-linux64.tar.gz && rm geckodriver-v0.30.0-linux64.tar.gz
COPY scrapers .

RUN mkdir raw_data log_dir
ENV RAW_FILES_DIR="/mair/raw_data" \
    SCRAPER_LOGS_DIR="/mair/log_dir" \
    GECKODRIVER_PATH="/mair/geckodriver" \
    LOGGING_LEVEL="INFO"

# Just to init airflow directory
RUN airflow version

RUN mkdir -p /root/airflow/dags && mv *.dag.py /root/airflow/dags/
