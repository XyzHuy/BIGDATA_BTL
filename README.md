# Hệ thống Giám sát Bệnh nhân ICU Thời gian thực cho Dự đoán Sepsis

## Giới thiệu

 Hệ thống được xây dựng nhằm mục tiêu giám sát liên tục các dấu hiệu sinh tồn (Vital signs) và chỉ số xét nghiệm (Laboratory values) của bệnh nhân trong khoa chăm sóc tích cực (ICU) và phát hiện sớm nguy cơ nhiễm trùng máu (sepsis) dựa trên dữ liệu PhysioNet/Computing in Cardiology Challenge 2019 (Sepsis Challenge). Hệ thống áp dụng kiến trúc stream processing sử dụng công nghệ Big Data và Machine Learning

## Nguồn dữ liệu

Dữ liệu đầu vào được mô phỏng từ bộ dữ liệu PhysioNet/Computing in Cardiology Challenge 2019, gồm các tệp .psv chứa chuỗi thời gian các chỉ số sinh tồn (HR, SBP, DBP, O2Sat, Temp, …) và nhãn sepsis theo từng giờ ICU (ICULOS). Bốn bệnh nhân được chọn làm mẫu và dữ liệu của họ được phát liên tục qua bốn luồng dữ liệu (streams) riêng biệt.

## Data flow
Dữ liệu được thu thập từ các tệp .psv, gửi qua Apache Kafka, xử lý và dự đoán bằng Apache Spark Streaming, lưu trữ kết quả vào Apache Cassandra, và cuối cùng cung cấp API truy vấn qua Flask để trực quan hóa trên giao diện web.

### Chi tiết:
Các file .psv được để trong producer/data. Trong đó producer/sepsis_producer.py có nhiệm vụ là giả lập một máy đo các dấu hiệu sinh tồn và chỉ số xét nghiệm và gửi cho kafka topic theo thời gian thực
Việc lựa chọn các bệnh nhân để theo dõi ở trong .env (tối đa 4 bệnh nhân)

Apache Spark Streaming đc để trong spark/app/spark_stream.py có tác dụng lấy các dữ liệu đang được gửi qua các topic trong kafka để xử lý (Preprocessing dữ liệu + inference model) và gửi dữ liệu đã đc xử lý cho cassandra

Cassandra gồm bảng sepsis_monitoring.icu_readings chứa các cột chứa các chỉ số & dấu hiệu mà spark đưa cho. Được khởi động bằng cassandra/init.cql

Flask sẽ phụ trách việc đọc các dữ liệu trong cassandra theo thời gian thực và cập nhật Highchart để visual dữ liệu. Nơi chứa các ENDPOINT (mã flask) ở trong api/app.py, frontend chứa các bảng highchart được để ở api/templates/dashboard.html

reload_server.py có tác dụng làm 1 server trung gian thực hiện restart một số container khi trigger nút reload simulation 





## Kiến trúc hệ thống

#### *Thu thập dữ liệu:*

Bốn producer (mỗi producer tương ứng một bệnh nhân) đọc tuần tự các dòng từ tệp .psv và gửi từng bản ghi dưới dạng JSON vào bốn topic Kafka riêng biệt (**icu_data_1** đến **icu_data_4**). Việc sử dụng Kafka đảm bảo tính đáng tin cậy và khả năng mở rộng

#### *Lưu trữ:*

Apache Cassandra được sử dụng làm hệ thống lưu trữ phân tán, tối ưu cho ghi/đọc dữ liệu theo thời gian thực. Schema được khởi tạo sẵn qua file**init.cql** với **keyspace icu_monitoring** và bảng lưu trữ dữ liệu bệnh nhân kèm kết quả dự đoán sepsis.

#### *Xử lý & Dự đoán:*


**Apache Spark Streaming** (phiên bản 3.5.0) tiêu thụ dữ liệu từ các topic Kafka, thực hiện:

* Tiền xử lý: làm sạch, windowing, chuẩn hóa đặc trưng, đưa về dạng chuẩn dữ liệu cho đầu vào của mô hình dự đoán
* Dự đoán thời gian thực bằng mô hình học máy đã được huấn luyện trước (lưu dưới dạng file `.h5`)
    REPO của mô hình huấn luyện: [Sepsis Prediction Model Training](https://github.com/nerajbobra/sepsis-prediction.git)
* Áp dụng ngưỡng phân loại dể dự đoán sepsis, chia ra các mức dương tính giả và dương tính thật.
* Ghi kết quả (dữ liệu thô + nhãn dự đoán + xác suất) vào Cassandra thông qua Spark Cassandra Connector.

#### *API & Visualize*
Ứng dụng Flask đóng vai trò trung gian, cung cấp REST API để truy vấn dữ liệu đã xử lý từ Cassandra theo ID bệnh nhân và khoảng thời gian. Hỗ trợ trả về dữ liệu theo định dạng phù hợp với biểu đồ thời gian (time-series), thuận tiện cho việc trực quan hóa và giám sát bằng Highcharts trên giao diện web.

