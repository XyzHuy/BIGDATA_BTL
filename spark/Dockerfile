FROM apache/spark:v3.2.3

USER root

# Cài pip3 (vì image chính thức không có sẵn)
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Đảm bảo lệnh `python` trỏ đến python3.7
RUN ln -sf /usr/bin/python3.7 /usr/bin/python

WORKDIR /app

COPY app/requirements.txt .

# Cài xgboost==0.90 trước (để dễ phát hiện lỗi)
RUN pip3 install --no-cache-dir xgboost==0.90 && \
    pip3 install --no-cache-dir -r requirements.txt

COPY app/ .

