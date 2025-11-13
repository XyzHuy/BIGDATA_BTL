from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
from pyspark.sql import DataFrame

import numpy as np
import pandas as pd
import pickle
from collections import deque


# 1. Load XGBoost model & stats

# Load XGBoost model
xgb_model = pickle.load(open("/app/models/f120d4e02n8010val434.pickle.dat", "rb"))

# Hard-coded stats from original model
varmeans = [84.58144338298742, 97.19395453339598, 36.977228240795384, 123.75046539637763, 82.40009988667639, 63.83055577034239, 18.72649785557987, 32.95765667291276, -0.6899191871174756, \
    24.075480562219358, 0.5548386348703284, 7.37893402619616, 41.02186880800917, 92.65418774854838, 260.22338482309493, 23.91545210569777, 102.48366144100076, 7.557530849328269, 105.82790991400108, \
        1.5106993531749389, 1.8361772575250843, 136.9322832898959, 2.646666023259181, 2.05145021490337, 3.544237652686153, 4.135527970939283, 2.114059461561731, 8.290099451999183, 30.79409334002751, 10.43083278791528, \
            41.231193461563706, 11.446405019759258, 287.38570591681315, 196.01391078961922, 62.00946887985519, 0.5592690422043409, 0.49657112470087744, 0.5034288752991226, -56.12512176894499, 26.994992301299437]

varstds = [17.3252, 2.9369, 0.77, 23.2316, 16.3418, 13.956, 5.0982, 7.9517, 4.2943, 4.3765, 11.1232, 0.0746, 9.2672, 10.893, 855.7468, 19.9943, 120.1227, 2.4332, 5.8805, 1.8056, 3.6941, 51.3107, 2.5262, 0.3979, 1.4233, 0.6421, 4.3115, 24.8062, 5.4917, 1.9687, 26.2177, 7.731, 153.0029, 103.6354, 16.3862, 0.4965, 0.5, 0.5, 162.2569, 29.0054]

varlogstds = [0.2069, 0.0338, 0.0209, 0.1862, 0.1929, 0.2133, 0.2811, 0.2632, 0.1, 0.1, 0.1, 0.0102, 0.2117, 0.1413, 1.3713, 0.6972, 0.6114, 0.6183, 0.0564, 0.6841, 1.4805, 0.3181, 0.6703, 0.1821, 0.3854, 0.1478, 1.0199, 2.723, 0.1788, 0.1896, 0.425, 0.5176, 0.5046, 0.5522, 0.3135, 0.1, 0.1, 0.1, 0.1, 0.9707]
varlogmeans = [4.4166, 4.576, 3.6101, 4.8009, 4.3928, 4.1334, 2.8926, 3.4633, 0.1, 0.1, 0.1, 1.9986, 3.6911, 4.5201, 4.104, 2.92, 4.3874, 1.9011, 4.6602, 0.1002, -0.5519, 4.8652, 0.7091, 0.7016, 1.1922, 1.4085, 0.0335, -0.8605, 3.4115, 2.327, 3.6051, 2.3098, 5.5347, 5.1412, 4.0841, 0.1, 0.1, 0.1, 0.1, 2.8862]

# Thứ tự cột PHẢI khớp với varmeans (40 cột)
feature_order = [
    'HR', 'O2Sat', 'Temp', 'SBP', 'MAP', 'DBP', 'Resp', 'EtCO2', 'BaseExcess', 'HCO3',
    'FiO2', 'pH', 'PaCO2', 'SaO2', 'AST', 'BUN', 'Alkalinephos', 'Calcium', 'Chloride',
    'Creatinine', 'Bilirubin_direct', 'Glucose', 'Lactate', 'Magnesium', 'Phosphate',
    'Potassium', 'Bilirubin_total', 'TroponinI', 'Hct', 'Hgb', 'PTT', 'WBC', 'Fibrinogen',
    'Platelets', 'Age', 'Gender', 'Unit1', 'Unit2', 'HospAdmTime', 'ICULOS'
]

log_indices = {14, 15, 16, 19, 20, 22, 25, 26, 27, 30, 31, 32}  # 0-based


def predict_sepsis_xgb(pdf: pd.DataFrame, pid: str):
    """
    Dự đoán sepsis bằng mô hình XGBoost.
    pdf: toàn bộ dữ liệu của bệnh nhân (đã sort theo ICULOS)
    """
    if len(pdf) == 0:
        return {'prob': 0.0, 'label': 0}

    try:
        data_40 = pdf[feature_order].values  # (T, 40)
    except KeyError as e:
        print(f"[ERROR] Missing columns for patient {pid}: {e}")
        return {'prob': 0.0, 'label': 0}

    data = np.copy(data_40)
    T, D = data.shape
    if D != 40:
        print(f"[ERROR] Expected 40 features, got {D} for {pid}")
        return {'prob': 0.0, 'label': 0}

    # Xử lý NaN
    nan_idx = np.where(np.isnan(data))
    mask = np.ones_like(data)
    data[nan_idx] = np.take(varmeans, nan_idx[1])
    mask[nan_idx] = 0

    # Forward-fill
    forward = np.copy(data[0, :])
    for t in range(T):
        for i in range(40):
            if mask[t, i] == 1:
                forward[i] = data[t, i]
            else:
                data[t, i] = forward[i]

    # Tính delta
    delta = np.zeros_like(data)
    for t in range(1, T):
        delta[t, :] = data[t, :] - data[t - 1, :]

    # Chuẩn hóa
    for i in range(40):
        if i in log_indices:
            # Đảm bảo giá trị > 0 trước khi log
            data[:, i] = np.clip(data[:, i], 1e-6, None)
            data[:, i] = 10 * (np.log(data[:, i]) - varlogmeans[i]) / varlogstds[i]
        else:
            data[:, i] = 10 * (data[:, i] - varmeans[i]) / varstds[i]

    # Ghép: data + delta + mask → (T, 120)
    full_data = np.concatenate([data, delta, mask], axis=1)  # (T, 120)

    # Tạo vector input: hiện tại + 5 trước (tổng 6 khung)
    row = list(full_data[-1, :])
    for j in range(1, 6):
        if T > j:
            row.extend(full_data[-(j + 1), :])
        else:
            row.extend([0.0] * 120)

    row = np.array([row])  # (1, 720)

    # Dự đoán
    try:
        dtest = xgb.DMatrix(row)
        pred_prob = float(xgb_model.predict(dtest)[0])
        pred_label = int(pred_prob >= 0.5)
        return {'prob': pred_prob, 'label': pred_label}
    except Exception as e:
        print(f"[ERROR] Prediction failed for {pid}: {e}")
        return {'prob': 0.0, 'label': 0}


# 2. Spark Schema & Session

icu_schema = StructType([
    StructField("HR", DoubleType(), True),
    StructField("O2Sat", DoubleType(), True),
    StructField("Temp", DoubleType(), True),
    StructField("SBP", DoubleType(), True),
    StructField("MAP", DoubleType(), True),
    StructField("DBP", DoubleType(), True),
    StructField("Resp", DoubleType(), True),
    StructField("EtCO2", DoubleType(), True),
    StructField("BaseExcess", DoubleType(), True),
    StructField("HCO3", DoubleType(), True),
    StructField("FiO2", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("PaCO2", DoubleType(), True),
    StructField("SaO2", DoubleType(), True),
    StructField("AST", DoubleType(), True),
    StructField("BUN", DoubleType(), True),
    StructField("Alkalinephos", DoubleType(), True),
    StructField("Calcium", DoubleType(), True),
    StructField("Chloride", DoubleType(), True),
    StructField("Creatinine", DoubleType(), True),
    StructField("Bilirubin_direct", DoubleType(), True),
    StructField("Glucose", DoubleType(), True),
    StructField("Lactate", DoubleType(), True),
    StructField("Magnesium", DoubleType(), True),
    StructField("Phosphate", DoubleType(), True),
    StructField("Potassium", DoubleType(), True),
    StructField("Bilirubin_total", DoubleType(), True),
    StructField("TroponinI", DoubleType(), True),
    StructField("Hct", DoubleType(), True),
    StructField("Hgb", DoubleType(), True),
    StructField("PTT", DoubleType(), True),
    StructField("WBC", DoubleType(), True),
    StructField("Fibrinogen", DoubleType(), True),
    StructField("Platelets", DoubleType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Gender", DoubleType(), True),
    StructField("Unit1", DoubleType(), True),
    StructField("Unit2", DoubleType(), True),
    StructField("HospAdmTime", DoubleType(), True),
    StructField("ICULOS", DoubleType(), True),
    StructField("patient_id", StringType(), True),
    StructField("icu_time_step", IntegerType(), True)
])

spark = SparkSession.builder \
    .appName("KafkaToCassandraWithModel") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 3. Đọc dữ liệu từ Kafka

topics = "icu_data_1,icu_data_2,icu_data_3,icu_data_4"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topics) \
    .option("maxOffsetsPerTrigger", 4) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), icu_schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("event_time", current_timestamp())


# 4. State và xử lý batch

patient_sequences = {}
patient_warning_state = {}

def process_batch(batch_df, batch_id):
    global patient_sequences, patient_warning_state
    print(f"=== Processing batch {batch_id} ===")
    
    batch_pd = batch_df.toPandas()
    if batch_pd.empty:
        return

    # Cập nhật state dữ liệu thô
    for pid, group in batch_pd.groupby('patient_id'):
        if pid not in patient_sequences:
            patient_sequences[pid] = group.copy()
        else:
            combined = pd.concat([patient_sequences[pid], group])
            combined = combined.drop_duplicates(subset=['ICULOS']).sort_values('ICULOS')
            # Giữ đủ lịch sử (không giới hạn cứng, hoặc có thể .tail(100) nếu cần)
            patient_sequences[pid] = combined

        if pid not in patient_warning_state:
            patient_warning_state[pid] = {
                'has_warning': False,
                'last_positive_iculos': -1,
                'first_warning_iculos': -1,
                'recent_labels': deque(maxlen=10),
                'ever_confirmed': False
            }

    output_rows = []
    for idx, row in batch_pd.iterrows():
        pid = row['patient_id']
        current_iculos = row['ICULOS']
        
        # Lấy toàn bộ lịch sử đến thời điểm hiện tại
        hist = patient_sequences[pid]
        hist_upto = hist[hist['ICULOS'] <= current_iculos].sort_values('ICULOS')
        
        # DỰ ĐOÁN BẰNG XGBOOST
        result = predict_sepsis_xgb(hist_upto, pid)
        raw_label = result['label']
        prob = result['prob']

        # Cập nhật state cảnh báo 
        state = patient_warning_state[pid]
        state['recent_labels'].append(raw_label)

        if raw_label == 1:
            state['last_positive_iculos'] = current_iculos

        # Reset warning
        if state['has_warning']:
            time_since_last_pos = current_iculos - state['last_positive_iculos']
            if (
                time_since_last_pos > 20 
                and all(x == 0 for x in state['recent_labels'])
                and not state['ever_confirmed']
            ):
                print(f"[{pid}] Reset warning at ICULOS={current_iculos}")
                state['has_warning'] = False
                state['last_positive_iculos'] = -1
                state['first_warning_iculos'] = -1
                state['recent_labels'].clear()
                    
        # Kích hoạt warning
        if not state['has_warning']:
            if sum(state['recent_labels']) >= 2:
                state['has_warning'] = True
                state['last_positive_iculos'] = current_iculos
                state['first_warning_iculos'] = current_iculos

        # Xác nhận confirmed
        is_confirmed = state['ever_confirmed']
        if state['has_warning']:
            if (
                current_iculos - state['first_warning_iculos'] > 20
                and state['last_positive_iculos'] != -1
                and current_iculos - state['last_positive_iculos'] <= 20
                and not all(x == 0 for x in state['recent_labels'])
            ):
                is_confirmed = True
                state['ever_confirmed'] = True

        # Gán kết quả
        row_dict = row.to_dict()
        row_dict['SepsisLabel'] = raw_label
        row_dict['SepsisProb'] = prob
        row_dict['SepsisWarning'] = int(state['has_warning'])
        row_dict['SepsisConfirmed'] = int(is_confirmed)

        output_rows.append(row_dict)
        print(f"[{pid}] ICULOS={current_iculos}, raw={raw_label}, has_warning={state['has_warning']}, "
              f"recent={list(state['recent_labels'])}, last_pos={state['last_positive_iculos']}, "
              f"first_warn={state['first_warning_iculos']}, confirmed={is_confirmed}")

    # Ghi vào Cassandra
    if output_rows:
        output_df = spark.createDataFrame(pd.DataFrame(output_rows))
        output_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="icu_readings", keyspace="sepsis_monitoring") \
            .save()
        print(f"→ Wrote {len(output_rows)} rows to Cassandra")


# 5. Start streaming

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='4 seconds') \
    .start()

query.awaitTermination()