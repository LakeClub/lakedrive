FROM python:3.7-slim-bullseye

ARG USER=app

RUN apt-get update -y \
    && apt-get -y install --no-install-recommends \
        build-essential \
    && python -m pip install --upgrade pip \
    && pip install \
        poetry==1.2.2 \
    && apt-get -y autoremove --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt /var/lib/dpkg/info /var/tmp/* /tmp/*

COPY bin/* /usr/local/bin/
RUN chmod +x /usr/local/bin/*

RUN useradd -m -U -d /app -r $USER
WORKDIR /app
USER $USER

COPY pyproject.toml .
ENV POETRY_VIRTUALENVS_PATH .pypoetry
RUN poetry update

COPY --chown=$USER . .

CMD tail -f /dev/null
