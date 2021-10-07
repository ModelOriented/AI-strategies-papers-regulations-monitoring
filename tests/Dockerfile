ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY tests tests

RUN mkdir raw_data
ENV RAW_FILES_DIR="/mair/raw_data" \
    LOGGING_LEVEL="DEBUG"

CMD poetry run pytest .
