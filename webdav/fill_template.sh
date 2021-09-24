#!/bin/bash
set -a
. /run/secrets/webdav_secrets
WEBDAV_HASH=`htpasswd -bnBC 10 "" ${WEBDAV_PASSWORD} | tr -d ':\n'`
envsubst < /config.template.yml > /config.yml
