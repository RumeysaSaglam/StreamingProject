FROM apache/airflow:2.8.1

# Root yetkisi ile sistem bağımlılıklarını kur
USER root

RUN apt-get update && \
    apt-get install -y gcc g++ libffi-dev libssl-dev librdkafka-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# airflow kullanıcısına geri dön
USER airflow

# Python bağımlılıklarını yükle
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH=/Users/rumeysasaglam/Projects
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/scripts"
