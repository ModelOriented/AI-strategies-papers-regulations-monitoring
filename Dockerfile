FROM python:3.8
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /mair

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.7 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    VENV_PATH="/opt/pysetup/.venv"

RUN mkdir raw_data log_dir

ENV RAW_FILES_DIR="/mair/raw_data" \
    SCRAPER_LOGS_DIR="/mair/log_dir" \
    GECKODRIVER_PATH="/mair/geckodriver"

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

COPY install_prerequirements.sh .
RUN sh install_prerequirements.sh

RUN poetry config virtualenvs.create false \
    # cleanup
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip

COPY pyproject.toml .
COPY poetry.lock .
RUN poetry install --no-interaction --no-ansi -vvv && rm -rf /root/.cache/pypoetry
COPY ./mars ./mars
RUN poetry install --no-interaction --no-ansi -vvv && rm -rf /root/.cache/pypoetry

# Copy DVC requirements
RUN mkdir ./data && mkdir ./models && mkdir ./models/tokenizers
COPY ./data/labels_hagendorffEthicsAIEthics2020.csv ./data/
COPY ./data/jobin2019.csv ./data/
COPY ./models/distilbert-base-uncased ./models/distilbert-base-uncased
COPY ./models/labse2 ./models/labse2
COPY ./models/universal-sentence-encoder-cmlm_multilingual-preprocess_2 ./models/universal-sentence-encoder-cmlm_multilingual-preprocess_2
COPY ./models/tokenizers/distilbert-base-uncased ./models/tokenizers/distilbert-base-uncased

ENV DATA_DIR=/mair/data \
    MODELS_DIR=/mair/models
