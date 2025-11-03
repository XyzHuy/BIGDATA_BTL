from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
from pyspark.sql import DataFrame

from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd


# load model pretrained
model = load_model('/app/models/model.h5')  # đường dẫn thực tế

# load thống kê mean/std
df_mean_std = pd.read_pickle('/app/models/mean_std_scaling.pkl')

# định nghĩa các cột như training
cols_cont = ['HR', 'MAP', 'O2Sat', 'SBP', 'Resp']
cols_to_bin = ['Unit1', 'Gender', 'HospAdmTime', 'Age', 'DBP', 'Temp', 'Glucose', 
                'Potassium', 'Hct', 'FiO2', 'Hgb', 'pH', 'BUN', 'WBC', 'Magnesium', 
                'Creatinine', 'Platelets', 'Calcium', 'PaCO2', 'BaseExcess', 'Chloride', 
                'HCO3', 'Phosphate', 'EtCO2', 'SaO2', 'PTT', 'Lactate', 'AST', 
                'Alkalinephos', 'Bilirubin_total', 'TroponinI', 'Fibrinogen', 'Bilirubin_direct']


# 1. Define Schema

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


# 2. Spark session

spark = SparkSession.builder \
    .appName("KafkaToCassandraWithModel") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 3. Read multiple Kafka topics

topics = "icu_data_1,icu_data_2,icu_data_3,icu_data_4"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", topics) \
    .option("maxOffsetsPerTrigger", 4) \
    .option("startingOffsets", "earliest") \
    .load()


# 4. Parse JSON

parsed_df = df.select(
    from_json(col("value").cast("string"), icu_schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn("event_time", current_timestamp())


# 5. Placeholder inference model

# Thêm dict lưu prediction state
patient_predictions = {}
def predict_sepsis_for_patient(pdf: pd.DataFrame, pid: str):
    pdf = pdf.sort_values('ICULOS').reset_index(drop=True)

    if len(pdf) < 10:
        return patient_predictions.get(pid, {'prob': 0.0, 'label': 0})

    # copy dữ liệu để xử lý inference
    group = pdf.iloc[-10:].copy()

    # 👉 Bỏ cột không dùng trong inference
    group_for_infer = group.drop(columns=['Unit2', 'ICULOS'], errors='ignore')

    # Fill NA, chuẩn hóa như trước
    for c in cols_cont:
        group_for_infer[c] = group_for_infer[c].fillna(method='bfill').fillna(method='ffill')

    if group_for_infer[cols_cont].isna().any().any():
        return patient_predictions.get(pid, {'prob': 0.0, 'label': 0})

    for c in cols_cont:
        group_for_infer[c] = (group_for_infer[c] - df_mean_std[c]['mean']) / df_mean_std[c]['std']

    # Tạo X_cont, X_cat
    X_cont = group_for_infer[cols_cont].values[np.newaxis, :, :]

    X_binned = {}
    for col in cols_to_bin:
        val = group_for_infer[col].dropna().median()
        if col not in ['Gender', 'Unit1']:
            val = (val - df_mean_std[col]['mean']) / df_mean_std[col]['std']
        X_binned[col] = val

    X_cat = np.array(list(X_binned.values()), dtype=np.float32)[np.newaxis, :]
    X_cat[np.isnan(X_cat)] = np.pi

    pred = model.predict([X_cont, X_cat], verbose=0)
    prob = float(pred[0][1])
    label = int(prob >= 0.5)

    patient_predictions[pid] = {'prob': prob, 'label': label}
    return {'prob': prob, 'label': label}



# 6. Function to process each micro-batch

from collections import deque

# State cho dữ liệu thô (để tạo window)
patient_sequences = {}

# State cho logic cảnh báo
patient_warning_state = {}  # {pid: { 'has_warning': bool, 'last_positive_iculos': float, 'recent_labels': deque }}

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
            combined = combined.drop_duplicates(subset=['ICULOS']).sort_values('ICULOS').tail(48)
            patient_sequences[pid] = combined

        # Khởi tạo state cảnh báo nếu chưa có
        if pid not in patient_warning_state:
            patient_warning_state[pid] = {
                'has_warning': False,
                'last_positive_iculos': -1,
                'first_warning_iculos': -1,  
                'recent_labels': deque(maxlen=10),
                'ever_confirmed': False
            }

    # Chuẩn bị danh sách kết quả để ghi
    output_rows = []

    # Xử lý TỪNG DÒNG trong batch gốc
    for idx, row in batch_pd.iterrows():
        pid = row['patient_id']
        current_iculos = row['ICULOS']
        
        #  1. Dự đoán raw từ model 
        hist = patient_sequences[pid]
        hist_upto = hist[hist['ICULOS'] <= current_iculos].sort_values('ICULOS')
        result = predict_sepsis_for_patient(hist_upto, pid)
        raw_label = result['label']
        prob = result['prob']

        #  2. Cập nhật state cảnh báo 
        state = patient_warning_state[pid]
        state['recent_labels'].append(raw_label)

        if raw_label == 1:
            state['last_positive_iculos'] = current_iculos

        #  3. Logic reset: nếu đã có warning nhưng >20h toàn 0 → reset 
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
                    

        #  4. Logic kích hoạt: nếu chưa có warning, nhưng có ≥2 lần 1 trong 10 dòng gần nhất 
        if not state['has_warning']:
            if sum(state['recent_labels']) >= 2:
                state['has_warning'] = True
                state['last_positive_iculos'] = current_iculos
                state['first_warning_iculos'] = current_iculos

        # Xác nhận true positive: nếu đã có warning và không reset trong 20h
        # Trong logic confirmed:
        is_confirmed = state['ever_confirmed']
            
        if state['has_warning']:
            if (
                current_iculos - state['first_warning_iculos'] > 20
                and state['last_positive_iculos'] != -1
                and current_iculos - state['last_positive_iculos'] <= 20
                and not all(x == 0 for x in state['recent_labels'])
            ):
                is_confirmed = True
                state['ever_confirmed'] = True  #  lưu lại trạng thái vĩnh viễn

        #  5. Gán kết quả 
        row_dict = row.to_dict()
        row_dict['SepsisLabel'] = raw_label          # raw từ model
        row_dict['SepsisProb'] = prob
        row_dict['SepsisWarning'] = int(state['has_warning'])  # cảnh báo đã xử lý
        row_dict['SepsisConfirmed'] = int(is_confirmed)

        output_rows.append(row_dict)
        print(f"[{pid}] ICULOS={current_iculos}, raw={raw_label}, has_warning={state['has_warning']}, "
        f"recent={list(state['recent_labels'])}, last_pos={state['last_positive_iculos']}, "
        f"first_warn={state['first_warning_iculos']}, confirmed={is_confirmed}")
        

    # Ghi toàn bộ dòng trong batch
    if output_rows:
        output_df = spark.createDataFrame(pd.DataFrame(output_rows))
        output_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="icu_readings", keyspace="sepsis_monitoring") \
            .save()

        print(f"→ Wrote {len(output_rows)} rows to Cassandra (with SepsisWarning)")



# 7. Start streaming query

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='4 seconds') \
    .start()

query.awaitTermination()