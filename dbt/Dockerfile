FROM debian:bookworm-slim

ARG DBT_VERSION=2.0.8

ENV PATH="/usr/local/bin:${PATH}"
ENV HOME="/root"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh \
    | sh -s -- --version "${DBT_VERSION}" --to /usr/local/bin \
    && test -x /usr/local/bin/dbt

WORKDIR /usr/app