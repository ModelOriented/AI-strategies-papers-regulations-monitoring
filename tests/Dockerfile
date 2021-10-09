ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY tests tests

ENV LOGGING_LEVEL="DEBUG"

CMD poetry run pytest .
