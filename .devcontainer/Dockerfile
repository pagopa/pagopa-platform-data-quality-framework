FROM python:3.11-slim

# installazione di Java, serve per PySpark
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_LOCAL_IP="127.0.0.1"

# setup cartella di lavoro
WORKDIR /app

# copia dei requirements e installazione
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copia di tutti i file
COPY . /app/

# variabili d'ambiente
ENV CONTRACTS_PATH="/app/datacontract"
ENV TABLE_LIMIT="50"
ENV DATA_SOURCE="RaptoreLake"

# avvia python
CMD python mock_data_setup.py && python data_quality.py