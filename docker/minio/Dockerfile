FROM quay.io/minio/minio

ARG MINIO_HOSTNAME=minio-dev
ENV CERTS_DIR /.minio/certs

RUN microdnf install \
    openssl \
    && microdnf clean all

RUN mkdir -p /mnt/"${MINIO_HOSTNAME}" \
    && mkdir -p ${CERTS_DIR} \
    && cd ${CERTS_DIR} \
    && openssl req -new -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/C=US/ST=NC/L=Local/O=Dev/CN=${MINIO_HOSTNAME}" -addext "subjectAltName = DNS:${MINIO_HOSTNAME}" -keyout ./private.key -out ./public.crt
