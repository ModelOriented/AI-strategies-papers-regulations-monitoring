#!/bin/bash
apt-get update && apt-get install -y --no-install-recommends freetds-bin krb5-user ldap-utils libffi7 libsasl2-2 libsasl2-modules libssl1.1 locales lsb-release sasl2-bin sqlite3 unixodbc firefox-esr libxml2 unzip libgbm1 fonts-liberation libasound2 libxshmfence1 xdg-utils
# geckodriver
wget -N https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz
tar xzvf geckodriver-v0.30.0-linux64.tar.gz
rm geckodriver-v0.30.0-linux64.tar.gz
# chrome driver
curl https://chromedriver.storage.googleapis.com/LATEST_RELEASE > version
wget https://chromedriver.storage.googleapis.com/$(cat version)/chromedriver_linux64.zip -O chromedriver.zip
unzip chromedriver.zip
rm chromedriver.zip
#chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -O chrome.deb
dpkg -i chrome.deb
rm chrome.deb
# gi
apt-get install -y python3-gi python3-gi-cairo gir1.2-secret-1 pkg-config libcairo2-dev libjpeg-dev libgif-dev libgirepository1.0-dev
