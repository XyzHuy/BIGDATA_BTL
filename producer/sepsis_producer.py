import os
import sys
import json
import time
import math
import pandas as pd
from kafka import KafkaProducer

def replace_nan_with_none(obj):
    """Recursively replace float('nan') with None for JSON compatibility."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(v) for v in obj]
    return obj


def stream_file(file_path, producer, topic, delay=2):
    df = pd.read_csv(file_path, sep='|')
    patient_id = os.path.basename(file_path).split('.')[0]

    for idx, row in df.iterrows():
        # Chuyển row thành dict, rồi xử lý NaN → None
        record = row.to_dict()
        record = replace_nan_with_none(record)  #  Đảm bảo không có NaN
        record["patient_id"] = patient_id
        record["icu_time_step"] = idx
        json_str = json.dumps(record, allow_nan=False)
        print("DEBUG JSON:", json_str[:200] + "...")

        producer.send(topic, value=record)
        print(f"[{patient_id}] Sent row {idx+1}/{len(df)} (SepsisLabel={record.get('SepsisLabel')})")
        time.sleep(delay)


def main():
    kafka_server = os.getenv("KAFKA_SERVER", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "icu_data")
    delay = float(os.getenv("DELAY", 2))

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = os.getenv("FILE_PATH", None)

    if not file_path or not os.path.exists(file_path):
        print("Bạncần truyền đường dẫn tới file .psv để stream")
        return

    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v, allow_nan=False).encode('utf-8')
    )

    try:
        stream_file(file_path, producer, topic, delay)
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
