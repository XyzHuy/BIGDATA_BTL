# producer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy code và data vào container
COPY sepsis_producer.py .

ARG HOST_DATA_DIR
ARG CONTAINER_DATA_DIR

COPY ${HOST_DATA_DIR} ${CONTAINER_DATA_DIR}

# Thiết lập biến môi trường mặc định
ENV KAFKA_SERVER=kafka:9092
ENV KAFKA_TOPIC=icu_data
ENV FILE_PATH=/data/p000001.psv
ENV DELAY=2

CMD ["python", "sepsis_producer.py"]
