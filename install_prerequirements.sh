#!/bin/bash
sudo apt install firefox libxml2 -y
wget -N https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz && tar xzvf geckodriver-v0.30.0-linux64.tar.gz && rm geckodriver-v0.30.0-linux64.tar.gz
sudo apt install gdebi-core
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo gdebi google-chrome-stable_current_amd64.deb