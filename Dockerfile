FROM bitnami/spark:latest
USER root
WORKDIR /app/spark_docker_sample
COPY pyproject.toml /app/spark_docker_sample/
RUN apt-get update \
        && apt-get install make \
        && apt-get install -y curl \
        && apt-get install -y python3.9 python3-pip \
        && curl -sSL https://install.python-poetry.org | POETRY_HOME=/etc/poetry python3 - \
        && export PATH=/etc/poetry/bin:$PATH \
        && poetry install --no-root
       



