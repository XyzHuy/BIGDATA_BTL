# 🏥 Hệ thống Giám sát Bệnh nhân ICU Thời Gian Thực Dựa Trên Dữ Liệu Sinh Lý Để Dự Đoán Nguy Cơ Nhiễm Trùng Huyết (Sepsis)
##  Table of Contents
- [0. Hướng dẫn chạy](#0-hướng-dẫn-chạy)
- [1. Giới thiệu](#1-giới-thiệu)
- [2. Nguồn dữ liệu](#2-nguồn-dữ-liệu)
- [3. Luồng dữ liệu (Data Flow)](#3-luồng-dữ-liệu-data-flow)
- [4. Tổng kết](#4-tổng-kết)

##  0. Hướng dẫn chạy
### 1.  Chọn các bệnh nhân để theo dõi:
- Mở file .env
- Gán giá trị cho biến PATIENT_ID tương ứng với bệnh nhân cần phân tích.

### 2.  Build Docker : 
- Chạy lệnh: docker-compose build
- Đợi spark-app ready

### 3.  Chạy toàn bộ hệ thống : 
- Chạy lệnh : docker compose up -d
<p align="center">
  <img src="https://raw.githubusercontent.com/XyzHuy/BIGDATA_BTL/main/Docker-Run.png" width="1000">
  <br>
  <em>Các container sau khi chạy thành công</em>
</p>

### 4.  Truy cập Dashboard : 
- Mở trình duyệt và truy cập: http://localhost:5000/dashboard
- Chọn Patient ID mong muốn
- Nhấn Apply và đợi Spark app tải batch dữ liệu để hiển thị biểu đồ trực quan (visualization)

<p align="center">
  <img src="https://raw.githubusercontent.com/XyzHuy/BIGDATA_BTL/main/DashBoard-1.png" width="1000">
  <br>
  <em></em>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/XyzHuy/BIGDATA_BTL/main/DashBoard-2.png" width="1000">
  <br>
  <em>Giao diện hiển thị</em>
</p>

### 5. Để chạy lại giả lập gửi dữ liệu stream cho spark:
- Stop container rồi run lại (trên Docker Desktop UI) hoặc docker compose down rồi chạy lệnh docker compose up -d

##  1. Giới thiệu

Hệ thống giám sát bệnh nhân ICU thời gian thực được phát triển với mục tiêu theo dõi liên tục các chỉ số sinh tồn (vital signs) và các thông số xét nghiệm (laboratory values) của bệnh nhân đang điều trị tại khoa Chăm sóc tích cực (ICU).

Mục tiêu chính của hệ thống là phát hiện sớm nguy cơ nhiễm trùng máu (sepsis) – một hội chứng có tỷ lệ tử vong cao nếu không được can thiệp kịp thời. Việc dự đoán sớm sepsis dựa trên dữ liệu thời gian thực có thể giúp các bác sĩ ra quyết định nhanh hơn, giảm thiểu biến chứng, và tối ưu hóa điều trị cho bệnh nhân.

Nguồn dữ liệu, kiến trúc và mô hình trí tuệ nhân tạo được xây dựng dựa trên bộ dữ liệu PhysioNet/Computing in Cardiology Challenge 2019 (Sepsis Challenge).
Hệ thống áp dụng kiến trúc Stream Processing kết hợp công nghệ Big Data và Machine Learning, cho phép xử lý dữ liệu ICU theo thời gian thực và mở rộng quy mô dễ dàng khi tích hợp thêm bệnh nhân hoặc ICU mới.

##  2. Nguồn dữ liệu

Dữ liệu đầu vào được mô phỏng từ bộ PhysioNet Sepsis Challenge 2019, bao gồm các tệp .psv (pipe-separated values). Mỗi tệp đại diện cho chuỗi thời gian của một bệnh nhân ICU, chứa các cột dữ liệu:

- Vital Signs: HR (Heart Rate), O2Sat (Oxygen Saturation), Temp (Temperature), SBP (Systolic Blood Pressure), DBP (Diastolic Blood Pressure), Resp (Respiratory Rate)

- Laboratory Values: WBC (White Blood Cells), Lactate, Creatinine, Platelets, v.v.

- Thông tin thời gian: ICULOS (số giờ kể từ khi bệnh nhân nhập ICU)

- Nhãn: sepsis_label (0 hoặc 1 – bệnh nhân bị sepsis hay không)

Trong hệ thống này, 4 bệnh nhân được chọn làm mẫu, và dữ liệu của họ được streaming liên tục qua 4 luồng dữ liệu độc lập (Kafka topics), tương ứng với 4 producer mô phỏng các thiết bị y tế tại giường bệnh. 

##  3. Luồng dữ liệu (Data Flow)

Quy trình hoạt động của hệ thống diễn ra theo pipeline sau:

    Crawl data -> Producer (.psv) -> Kafka -> Spark Streaming -> Cassandra -> Flask API -> Highcharts Dashboard
    
<p align="center">
  <img src="https://raw.githubusercontent.com/XyzHuy/BIGDATA_BTL/main/Sơ-Đồ-Thiết-Kế-Hệ-Thống.png" width="1000">
  <br>
  <em>Hình ảnh mô tả pipeline hoạt động</em>
</p>

 Chi tiết luồng hoạt động: 

### 1.  Producer (producer/sepsis_producer.py):

- Mỗi producer đọc tuần tự từng dòng trong file .psv.

- Giả lập thiết bị đo sinh tồn tại giường bệnh.

- Gửi từng bản ghi dữ liệu (theo thời gian thực) dưới dạng JSON đến Kafka Topic tương ứng (icu_data_1 đến icu_data_4).

- Các bệnh nhân được chọn để mô phỏng được cấu hình trong file .env (tối đa 4 bệnh nhân đồng thời).

### 2.  Kafka Broker:

- Đóng vai trò trung gian truyền dữ liệu theo mô hình publish–subscribe.

- Đảm bảo tính toàn vẹn, độ trễ thấp và khả năng mở rộng khi nhiều producer và consumer hoạt động song song.

### 3.  Spark Streaming Application (spark/app/spark_stream.py):

- Là consumer chính nhận dữ liệu từ Kafka.

- Thực hiện chuỗi tiền xử lý (preprocessing):

- Làm sạch dữ liệu bị thiếu.

- Chuẩn hóa và chuẩn bị feature vector cho mô hình.

- Cửa sổ thời gian (windowing) để phân tích chuỗi tín hiệu liên tục.

Sau khi tiền xử lý, Spark gọi mô hình học máy đã huấn luyện sẵn để dự đoán xác suất sepsis tại thời điểm đó.

Ghi kết quả gồm:

- Giá trị gốc (vital signs, lab values)

- Xác suất xem bệnh nhân có bị nhiễm sepsis hay không ? 

- Nhãn dự đoán (sepsis_pred_label)

- Thời gian đo (timestamp)

Toàn bộ kết quả được ghi trực tiếp vào Apache Cassandra thông qua Spark Cassandra Connector.

### 4.  Apache Cassandra (cassandra/init.cql):

- Lưu trữ dữ liệu đầu ra trong bảng icu_readings thuộc keyspace sepsis_monitoring.

Cấu trúc dữ liệu tối ưu cho truy vấn thời gian thực theo:

    patient_id | timestamp | vital_signs | lab_values | sepsis_prob | sepsis_label

Thiết kế theo mô hình distributed column store, đảm bảo hiệu suất đọc/ghi cao và khả năng mở rộng khi dữ liệu ICU tăng nhanh.  

### 5.  Flask API Server (api/app.py):

- Đóng vai trò trung gian giữa Cassandra và Frontend.
- Cung cấp các RESTful API endpoint cho phép truy vấn dữ liệu:
- Theo ID bệnh nhân
- Theo khoảng thời gian (timestamp range)
- Kết quả được trả về dạng JSON, phù hợp cho frontend vẽ biểu đồ thời gian thực.
- API cũng hỗ trợ endpoint để reload simulation (khi cần khởi động lại toàn bộ pipeline). 

### 6.  Frontend Visualization (Highcharts Dashboard) (api/templates/dashboard.html):

- Hiển thị dữ liệu sinh tồn, xét nghiệm và kết quả dự đoán sepsis theo thời gian thực.

- Sử dụng Highcharts để trực quan hóa các chỉ số (HR, O2Sat, Temp, SBP, DBP, v.v.).

- Các biểu đồ sử dụng đồ thị dạng line và area, với màu sắc phản ánh nguy cơ sepsis (ví dụ vùng đỏ nhạt cho sepsis confirmed).

- Giao diện tự động cập nhật định kỳ (polling từ Flask API) để hiển thị dữ liệu mới.



## 4. Tổng kết

Hệ thống giám sát sepsis thời gian thực cho bệnh nhân ICU là một giải pháp kết hợp công nghệ Big Data, xử lý luồng (stream processing) và Machine Learning để hỗ trợ y tế chủ động.
Toàn bộ pipeline được thiết kế mô-đun hóa, dễ mở rộng, và có thể triển khai trên môi trường Docker Compose hoặc Kubernetes.
