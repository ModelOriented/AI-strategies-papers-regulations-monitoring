#!/bin/bash
apt-get update && apt-get install -y --no-install-recommends freetds-bin krb5-user ldap-utils libffi7 libsasl2-2 libsasl2-modules libssl1.1 locales lsb-release sasl2-bin sqlite3 unixodbc firefox-esr libxml2 unzip
wget -N https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz && tar xzvf geckodriver-v0.30.0-linux64.tar.gz && rm geckodriver-v0.30.0-linux64.tar.gz
wget https://chromedriver.storage.googleapis.com/95.0.4638.17/chromedriver_linux64.zip -O chromedriver.zip && unzip chromedriver.zip && rm chromedriver.zip
