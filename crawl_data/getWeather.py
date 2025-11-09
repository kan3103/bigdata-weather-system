import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from address import add_latlon_to_json

INPUT_FILE = "data/vietnam_addresses_with_latlon.json"  # file JSON có sẵn lat/lon
OUTPUT_FILE = "data/weather_data.json"


# ===== Hàm lấy weather từ Open-Meteo
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

# ===== Hàm worker cho ThreadPool
def fetch_weather_for_location(location):
    lat = location.get("lat")
    lon = location.get("lon")
    if lat is None or lon is None:
        return location  # skip nếu không có tọa độ
    location["weathers"] = get_weather(lat, lon)
    print(f"Lấy weather xong cho {location['addr']}")
    return location

def run_loop():
    while True:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Chuyển dữ liệu thành danh sách địa điểm (lat/lon + addr)
        addresses = []

        for province_name, province_data in raw_data.items():
            # Bậc 1: tỉnh
            addr = {
                "addr": province_name,
                "lat": province_data.get("lat"),
                "lon": province_data.get("lon"),
                "level": 1  # bậc 1 = tỉnh
            }
            addresses.append(addr)

            # Bậc 2: huyện/quận/thành phố thuộc tỉnh
            for ward in province_data.get("wards", []):
                addr = {
                    "addr": f"{ward['name']}, {province_name}",
                    "lat": ward.get("lat"),
                    "lon": ward.get("lon"),
                    "level": 2  # bậc 2 = huyện/quận
                }
                addresses.append(addr)

        print(f"Tổng số địa điểm: {len(addresses)}")

        locations = {"timestamps": datetime.now().isoformat(), "addresses": []}

        max_threads = 10  
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_addr = {executor.submit(fetch_weather_for_location, addr): addr for addr in addresses}
            for future in as_completed(future_to_addr):
                location_with_weather = future.result()
                locations["addresses"].append(location_with_weather)

        # Ghi kết quả ra file JSON
        locations["timestamps"] = datetime.now().isoformat()
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(locations, f, ensure_ascii=False, indent=4)

        print(f"\n Hoàn tất! Dữ liệu weather đã lưu vào: {OUTPUT_FILE}")
        
        time.sleep(5*60)


if __name__ == "__main__":
    add_latlon_to_json()
    run_loop()