import requests
import json
import time
import os # <-- Thêm import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import Kafka ---
from kafka import KafkaProducer # type: ignore
from kafka.errors import KafkaTimeoutError # type: ignore

# --- Config Kafka ---
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'du_lieu_khu_vuc'

# --- Config Files ---
# Dùng os.path.join để đảm bảo đường dẫn đúng
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_FILE_ADDRESS = os.path.join(DATA_DIR, "southern_vn.json")
INPUT_FILE_LATLON = os.path.join(DATA_DIR, "vietnam_addresses_with_latlon.json")
OUTPUT_FILE_WEATHER = os.path.join(DATA_DIR, "weather_data.json")

# ==========================================================
# GIAI ĐOẠN 1: CÁC HÀM LẤY ĐỊA CHỈ (TỪ ADDRESS.PY)
# ==========================================================

BASE_URL = "https://provinces.open-api.vn/api/v2"
southern_keywords = [
    "Hồ Chí Minh"
    # , "Đồng Nai", "Tây Ninh", "Đồng Tháp", "An Giang", "Vĩnh Long", "Cần Thơ", "Cà Mau"
]

def get_all_provinces():
    res = requests.get(f"{BASE_URL}/p/")
    res.raise_for_status()
    return res.json()

def get_province_details(code):
    res = requests.get(f"{BASE_URL}/p/{code}?depth=2")
    res.raise_for_status()
    return res.json()

def get_address_api():
    print("--- Bắt đầu lấy danh sách Tỉnh/Huyện từ API ---")
    all_provinces = get_all_provinces()
    data_south = {}

    for p in all_provinces:
        if any(keyword in p["name"] for keyword in southern_keywords):
            print(f"Đang lấy dữ liệu: {p['name']}")
            try:
                detail = get_province_details(p["code"])
                data_south[p["name"]] = detail
                time.sleep(0.5) 
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu {p['name']}: {e}")
    
    with open(INPUT_FILE_ADDRESS, "w", encoding="utf-8") as f:
        json.dump(data_south, f, ensure_ascii=False, indent=2)

    print(f"Đã lưu vào {INPUT_FILE_ADDRESS}")
    print(f"Tổng số tỉnh miền Nam lấy được: {len(data_south)}")

def get_latlon(address: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address + ", Việt Nam",
        "format": "json",
        "limit": 1
    }
    headers = {"User-Agent": "Mozilla/5.0"} # API này yêu cầu User-Agent
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    if not data:
        print(f"Không tìm thấy địa chỉ: {address}")
        return None, None
    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    return lat, lon

def add_latlon_to_json():
    print(f"\n--- Bắt đầu thêm Lat/Lon vào file ---")
    try:
        with open(INPUT_FILE_ADDRESS, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file {INPUT_FILE_ADDRESS}. Hãy chạy get_address_api() trước.")
        return

    for province_name, province_data in data.items():
        print(f"\nXử lý {province_name}...")
        lat, lon = get_latlon(province_name)
        province_data["lat"] = lat
        province_data["lon"] = lon
        time.sleep(1) # Ngủ 1s để không bị block IP

        for ward in province_data.get("wards", []):
            ward_name = ward["name"]
            full_address = f"{ward_name}, {province_name}"
            print(f"  Đang lấy tọa độ cho: {full_address}")
            lat, lon = get_latlon(full_address)
            ward["lat"] = lat
            ward["lon"] = lon
            time.sleep(1) # Ngủ 1s
    
    with open(INPUT_FILE_LATLON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nHoàn tất! Dữ liệu có tọa độ được lưu trong: {INPUT_FILE_LATLON}")


# ==========================================================
# GIAI ĐOẠN 2: CÁC HÀM KAFKA & WEATHER
# ==========================================================

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
    """Hàm lấy weather từ Open-Meteo"""
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
    """Hàm worker cho ThreadPool"""
    lat = location.get("lat")
    lon = location.get("lon")
    if lat is None or lon is None:
        return location
    
    location["weathers"] = get_weather(lat, lon) 
    print(f"Lấy weather xong cho {location['addr']}")
    return location

def run_loop():
    print("\n--- Khởi động vòng lặp Producer ---")
    producer = create_kafka_producer()
    if not producer:
        print("Không thể kết nối Kafka. Thoát.")
        return

    while True:
        try:
            with open(INPUT_FILE_LATLON, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file config {INPUT_FILE_LATLON}")
            print("Vui lòng chạy 2 hàm setup (get_address_api, add_latlon_to_json) trước.")
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

        print(f"\n--- Bắt đầu chu kỳ cào dữ liệu ---")
        print(f"Tổng số địa điểm: {len(addresses)}")

        locations = {"timestamps": datetime.now().isoformat(), "addresses": []}
        max_threads = 10  
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_addr = {executor.submit(fetch_weather_for_location, addr): addr for addr in addresses}
            for future in as_completed(future_to_addr):
                location_with_weather = future.result()
                locations["addresses"].append(location_with_weather)

        locations["timestamps"] = datetime.now().isoformat()
        with open(OUTPUT_FILE_WEATHER, "w", encoding="utf-8") as f:
            json.dump(locations, f, ensure_ascii=False, indent=4)
        print(f"\nHoàn tất! Dữ liệu weather đã lưu vào: {OUTPUT_FILE_WEATHER}")
        
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

        print("\n--- Hoàn tất chu kỳ. Ngủ 5 phút ---")
        time.sleep(5*60)


if __name__ == "__main__":
    # 0. Tạo thư mục data nếu chưa có
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # 1. Chạy setup 1 lần để lấy tọa độ
    # (Comment 2 dòng này lại sau khi chạy thành công lần đầu)
    get_address_api() 
    add_latlon_to_json()
    
    # 2. Chạy vòng lặp producer
    run_loop()