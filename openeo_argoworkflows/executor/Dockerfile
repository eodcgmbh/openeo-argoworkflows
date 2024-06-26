FROM python:3.11-slim as base

WORKDIR /opt
ARG POETRY_VERSION="1.8.3"

ENV USERNAME=executor
ENV POETRY_VIRTUALENVS_IN_PROJECT 1
ENV POETRY_HOME=/opt/poetry
ENV VIRTUAL_ENV=/opt/openeo_argoworkflows_executor/.venv

COPY ./README.md ./pyproject.toml ./poetry.lock /opt/

RUN python3 -m venv ENV_DIR $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install -U pip setuptools && \
    $VIRTUAL_ENV/bin/pip install poetry==${POETRY_VERSION}

ENV PATH="${POETRY_HOME}/bin:${VIRTUAL_ENV}/bin:${PATH}"

RUN poetry install --only main

COPY --chown=${USER_ID}:${GROUP_ID} ./openeo_argoworkflows_executor /opt/openeo_argoworkflows_executor

USER ${USER_ID}

WORKDIR /opt/openeo_argoworkflows_executor

CMD ["openeo_executor"]