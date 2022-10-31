FROM python:3.7-slim-bullseye

ARG USER=app

RUN apt-get update -y \
    && apt-get -y install --no-install-recommends \
        build-essential \
    && python -m pip install --upgrade pip \
    && pip install \
        Sphinx==5.1.1 \
        sphinx-rtd-theme==1.0.0 \
        fastapi==0.85.2 \
        uvicorn==0.19.0 \
    && apt-get -y autoremove --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt /var/lib/dpkg/info /var/tmp/* /tmp/*

RUN useradd -m -U -d /app -r $USER
WORKDIR /app
USER $USER

COPY --chown=$USER . .
RUN make -C docs html

COPY --chown=$USER docker/docs/app .
CMD uvicorn main:app --host 0.0.0.0 --port 8888
