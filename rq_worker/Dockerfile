ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY rq_worker rq_worker
CMD ["python", "rq_worker/app.py"]
