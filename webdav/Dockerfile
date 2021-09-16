FROM micromata/dave
USER root

COPY config.template.yml /
COPY fill_template.sh /bin/fill_template.sh

RUN apk add bash && apk add gettext && apk add apache2-utils && chmod +x /bin/fill_template.sh && mkdir /storage
ENTRYPOINT bash -c "fill_template.sh && /usr/local/bin/dave"
