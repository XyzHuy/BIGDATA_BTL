FROM apache/spark:3.5.0

USER root


RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

#  Copy requirements trước để cache pip install
COPY app/requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY app/ .

USER spark

