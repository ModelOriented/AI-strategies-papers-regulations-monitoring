ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY webserver webserver
EXPOSE 5000
CMD ["python", "webserver/app.py"]
