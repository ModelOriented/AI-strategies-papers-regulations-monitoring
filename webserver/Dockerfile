ARG docker_user
ARG docker_tag
FROM ${docker_user}/common:${docker_tag}

COPY webserver webserver

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh | bash
RUN . /root/.bashrc && nvm install lts/fermium && nvm install-latest-npm
RUN cd webserver/frontend && . /root/.bashrc && nvm use lts/fermium && npm install && npm run build && cp -R dist ../static

EXPOSE 5000
CMD ["python", "webserver/app.py"]
