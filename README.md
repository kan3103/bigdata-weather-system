# bigdata-weather-system

# Hướng dẫn chạy Producer


## Bước 1: Khởi chạy Môi trường Kafka (Chỉ làm 1 lần)

1.  Mở terminal

2.  Khởi động Kafka, Zookeeper, và Kafdrop (giao diện web):
    ```bash
    docker-compose up -d
    ```
3.  (Tùy chọn) Kiểm tra xem 3 container đã chạy (màu xanh) chưa bằng Docker Desktop.

---

## Bước 2: Tạo Topic (Chỉ làm 1 lần)

1.  Vẫn ở thư mục gốc, chạy lệnh sau để tạo topic `du_lieu_khu_vuc`:
    ```bash
    docker-compose exec kafka kafka-topics --create --topic du_lieu_khu_vuc --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
    ```
    *(**Ghi chú:** `--partitions 1` do hiện tại cần ít thôi, trong tương lai mở rộng ra thì sẽ thêm partition còn giờ hơi tốn RAM )*

---

## Bước 3: Cài đặt thư viện Python (Chỉ làm 1 lần)

1.  `cd` vào thư mục `crawl_data`:
    ```bash
    cd crawl_data
    ```
2.  Cài đặt các thư viện cần thiết:
    ```bash
    pip install -r requirements.txt
    ```

---

## Bước 4: Chạy Script Producer (Để gửi dữ liệu)

1.  Đảm bảo bạn đang ở trong thư mục `crawl_data`.
2.  Chạy file `producer.py`:
    ```bash
    python producer.py
    ```
3.  Terminal này sẽ bắt đầu cào và gửi dữ liệu vào Kafka theo chu kỳ. Cứ để nó chạy.

---

## Bước 5: Kiểm tra Dữ liệu

Sau khi `producer.py` đã chạy, bạn có 2 cách để xem dữ liệu:

### Cách 1: Dùng Giao diện Web (Kafdrop)

* Mở trình duyệt và truy cập: **`http://localhost:9000`**
* Bấm vào topic `du_lieu_khu_vuc`.
* Bấm vào `View Messages` để xem các tin nhắn JSON.

### Cách 2: Dùng Terminal (Console Consumer)

1.  Mở một **terminal mới** ở file tổng.
2.  Chạy lệnh này:
    ```bash
    docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic du_lieu_khu_vuc --from-beginning
    ```
3.  Terminal này sẽ "lắng nghe" và in ra bất kỳ tin nhắn JSON *mới* nào được gửi đến.