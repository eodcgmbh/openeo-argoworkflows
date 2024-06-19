FROM python:3.11-slim as prod

WORKDIR /opt

ARG USER_ID=1000
ARG GROUP_ID=1000
ARG POETRY_VERSION="1.8.3"

ENV USERNAME=api
ENV POETRY_VIRTUALENVS_IN_PROJECT 1
ENV POETRY_HOME=/opt/poetry
ENV VIRTUAL_ENV=/opt/openeo_argoworkflows/.venv

COPY ./README.md ./pyproject.toml ./poetry.lock /opt/

RUN python3 -m venv ENV_DIR $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install -U pip setuptools && \
    $VIRTUAL_ENV/bin/pip install poetry==${POETRY_VERSION}

ENV PATH="${POETRY_HOME}/bin:${VIRTUAL_ENV}/bin:${PATH}"

RUN poetry install --only main

COPY --chown=${USER_ID}:${GROUP_ID} ./openeo_argoworkflows /opt/openeo_argoworkflows

RUN addgroup --gid ${GROUP_ID} ${USERNAME}
RUN adduser --disabled-password --gecos '' --uid ${USER_ID} --gid ${GROUP_ID} ${USERNAME}

USER ${USER_ID}

WORKDIR /opt/openeo_argoworkflows

CMD ["uvicorn", "openeo_argoworkflows.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "8"]
