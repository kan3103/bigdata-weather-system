import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import Kafka ---
from address import add_latlon_to_json, get_address_api
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

# --- Config Kafka  ---
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'du_lieu_khu_vuc'

# --- Config Files ---
INPUT_FILE = "data/vietnam_addresses_with_latlon.json"
OUTPUT_FILE = "data/weather_data.json"

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
        producer.send(topic, key=key_bytes, value=data)
        
        location_name = data.get("location_name", "Unknown")
        print(f"Đã gửi thành công dữ liệu cho: {location_name}")
    except Exception as e:
        print(f"Lỗi khi gửi dữ liệu: {e}")


def get_weather(lat: float, lon: float):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": True
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("current_weather", {})
    except Exception as e:
        print(f"Lỗi khi lấy weather cho {lat},{lon}: {e}")
        return {}

def fetch_weather_for_location(location):
    lat = location.get("lat")
    lon = location.get("lon")
    if lat is None or lon is None:
        return location
    
    location["weathers"] = get_weather(lat, lon) 
    print(f"Lấy weather xong cho {location['addr']}")
    return location

def run_loop():
    producer = create_kafka_producer()
    if not producer:
        print("Không thể kết nối Kafka. Thoát.")
        return

    while True:
        try:
            with open(INPUT_FILE, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file config {INPUT_FILE}")
            time.sleep(60)
            continue

        addresses = []
        for province_name, province_data in raw_data.items():
            addr_tinh = {
                "addr": province_name,
                "lat": province_data.get("lat"),
                "lon": province_data.get("lon"),
                "level": 1
            }
            addresses.append(addr_tinh)

            for ward in province_data.get("wards", []):
                addr_huyen = {
                    "addr": f"{ward['name']}, {province_name}",
                    "lat": ward.get("lat"),
                    "lon": ward.get("lon"),
                    "level": 2
                }
                addresses.append(addr_huyen)

        print(f"Tổng số địa điểm: {len(addresses)}")

        locations = {"timestamps": datetime.now().isoformat(), "addresses": []}
        max_threads = 10  
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_addr = {executor.submit(fetch_weather_for_location, addr): addr for addr in addresses}
            for future in as_completed(future_to_addr):
                location_with_weather = future.result()
                locations["addresses"].append(location_with_weather)

        locations["timestamps"] = datetime.now().isoformat()
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(locations, f, ensure_ascii=False, indent=4)
        print(f"\nHoàn tất! Dữ liệu weather đã lưu vào: {OUTPUT_FILE}")
        
        print("--- Bắt đầu gửi lên Kafka ---")
        for loc_data in locations["addresses"]:
            
            current_weather_data = loc_data.get("weathers")
            
            if not current_weather_data:
                print(f"Bỏ qua {loc_data.get('addr')} vì không có dữ liệu weather.")
                continue

            message = {
                "location_name": loc_data.get("addr"),
                "level": loc_data.get("level"),
                "latitude": loc_data.get("lat"),
                "longitude": loc_data.get("lon"),
                
                **current_weather_data 
            }
            
            send_data_to_kafka(producer, KAFKA_TOPIC, message)

        try:
            producer.flush(timeout=10) 
            print("Đã flush producer.")
        except KafkaTimeoutError:
            print("LỖI: Kafka flush timeout! Server bị treo. Bỏ qua chu kỳ này.")
        except Exception as e:
            print(f"LỖI: Không thể flush: {e}")

        print("\n--- Hoàn tất chu kỳ (mỗi 5p). ---")
        time.sleep(5*60)


if __name__ == "__main__":
    get_address_api() 
    add_latlon_to_json()
    run_loop()