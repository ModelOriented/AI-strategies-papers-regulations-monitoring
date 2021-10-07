#!/bin/bash
sudo apt install firefox libxml2 -y
wget -N https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz && tar xzvf geckodriver-v0.30.0-linux64.tar.gz && rm geckodriver-v0.30.0-linux64.tar.gz
