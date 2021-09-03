FROM python:3.7.1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.0.5 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    VENV_PATH="/opt/pysetup/.venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    # deps for installing poetry
    curl \
    # deps for building python deps
    build-essential \
    # install poetry - uses $POETRY_VERSION internally
    && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python \
    && poetry --version \
    && pip install --upgrade pip \
    # configure poetry & make a virtualenv ahead of time since we only need one
    && python -m venv $VENV_PATH

RUN poetry config virtualenvs.create false \
    # cleanup
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
# copy project requirement files here to ensure they will be cached.
WORKDIR /src
COPY poetry.lock pyproject.toml ./
# RUN poetry init
RUN poetry run pip install --upgrade pip
RUN poetry run pip install --upgrade setuptools
RUN poetry install --no-interaction --no-ansi -vvv

COPY ./mars /src/mars
RUN poetry install --no-interaction --no-ansi -vvv

COPY ./app.py .
COPY ./.env .
EXPOSE 8080
ENTRYPOINT ["poetry", "run"]
