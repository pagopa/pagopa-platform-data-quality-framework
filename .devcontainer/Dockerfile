FROM python:3.11-slim

# installazione di Java (richiesto da PySpark) e make
RUN apt-get update && \
    apt-get install -y default-jre procps make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_LOCAL_IP="127.0.0.1"

WORKDIR /app

# layer dipendenze separato per sfruttare la cache Docker
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# copia del sorgente e installazione in modalità editable
# (rende il pacchetto importabile come `dq_framework.*`)
# L'extra [dev] aggiunge pytest, pytest-cov, ruff (definiti in pyproject.toml)
# in modo che `python -m pytest` sia disponibile out-of-the-box nel container.
COPY . /app/
RUN pip install --no-cache-dir -e ".[dev]"

ENV CONTRACTS_PATH="/app/tests/fixtures/contracts"
ENV TABLE_LIMIT="50"
ENV DATA_SOURCE="RaptoreLake"