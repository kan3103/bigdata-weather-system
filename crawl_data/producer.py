import json
import time
from kafka import KafkaProducer

from weatherAPI import WeatherAPI
from main import get_weather_for_multiple_locations

KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'du_lieu_khu_vuc'
FILE_CONFIG_KHU_VUC = 'location.json'

# Danh sách các biến thời tiết
VARIABLES = [
    "temperature_2m", 
    "relative_humidity_2m", 
    "apparent_temperature", 
    "precipitation"
]
# ------------------------------------------------

def create_kafka_producer():
    """Khởi tạo Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(">>> Đã kết nối tới Kafka Producer.")
        return producer
    except Exception as e:
        print(f"Lỗi khi khởi tạo Kafka Producer: {e}")
        return None

def send_data_to_kafka(producer, topic, data):
    """Gửi một bản ghi (data) đến Kafka topic"""
    try:
        key_bytes = data['location_name'].encode('utf-8')
        producer.send(topic,key=key_bytes, value=data)
        location_name = data.get("location_name", "Unknown")
        print(f"Đã gửi thành công dữ liệu cho: {location_name}")
    except Exception as e:
        print(f"Lỗi khi gửi dữ liệu: {e}")

def load_khu_vuc_config(file_path):
    """Hàm đọc danh sách khu vực từ file JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            khu_vuc_list = json.load(f)
            print(f"--- Đã tải thành công {len(khu_vuc_list)} khu vực từ {file_path} ---")
            return khu_vuc_list
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file config {file_path}")
        return []
    except Exception as e:
        print(f"Lỗi khi đọc file config: {e}")
        return []

if __name__ == "__main__":
    
    # 1. Tải danh sách khu vực
    khu_vuc_list = load_khu_vuc_config(FILE_CONFIG_KHU_VUC)
    
    # 2. Khởi tạo Kafka Producer
    producer = create_kafka_producer()
    
    # 3. Khởi tạo Weather API
    weather_api = WeatherAPI()
    
    if producer and khu_vuc_list and weather_api:
        while True:
            print(f"\n--- Bắt đầu chu kỳ cào dữ liệu ---")
            
            # 4. GỌI HÀM 
            all_weather_data = get_weather_for_multiple_locations(
                weather_api, 
                khu_vuc_list, 
                variables=VARIABLES
            )
            
            print(f"--- Đã cào xong {len(all_weather_data)} khu vực ---")
            
            # 5. Gửi từng kết quả vào Kafka
            for data in all_weather_data:
                send_data_to_kafka(producer, KAFKA_TOPIC, data)

            
            try:
                # Chỉ chờ tối đa 10 giây
                producer.flush(timeout=10) 
                print("Đã flush producer.")
            except KafkaTimeoutError:
                print("LỖI: Kafka flush timeout! Server bị treo. Bỏ qua chu kỳ này.")
            except Exception as e:
                print(f"LỖI: Không thể flush: {e}")
            
            time.sleep(10)
    else:
        print("Không thể khởi động. Vui lòng kiểm tra Kafka, Config file hoặc Weather API.")