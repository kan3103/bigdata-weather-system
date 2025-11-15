import requests
import json
import time
import os

BASE_URL = "https://provinces.open-api.vn/api/v2"

southern_keywords = [
    "Hồ Chí Minh"
    #  , "Đồng Nai", "Tây Ninh", "Đồng Tháp", "An Giang", "Vĩnh Long", "Cần Thơ", "Cà Mau"
]
INPUT_FILE = "data/southern_vn.json"      
OUTPUT_FILE = "data/vietnam_addresses_with_latlon.json"  

def get_all_provinces():
    res = requests.get(f"{BASE_URL}/p/")
    res.raise_for_status()
    return res.json()

def get_province_details(code):
    res = requests.get(f"{BASE_URL}/p/{code}?depth=2")
    res.raise_for_status()
    return res.json()

def get_address_api():
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
    
    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data_south, f, ensure_ascii=False, indent=2)

    print(f"Đã lưu vào {INPUT_FILE}")
    print(f"Tổng số tỉnh miền Nam lấy được: {len(data_south)}")
    


def get_latlon(address: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address + ", Việt Nam",
        "format": "json",
        "limit": 1
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    if not data:
        print(f"Không tìm thấy địa chỉ: {address}")
        return None
    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    return lat, lon

def add_latlon_to_json():
    if os.path.exists(OUTPUT_FILE):
        return
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    for province_name, province_data in data.items():
        print(f"\nXử lý {province_name}...")
        lat, lon = get_latlon(province_name)
        province_data["lat"] = lat
        province_data["lon"] = lon

        for ward in province_data.get("wards", []):
            ward_name = ward["name"]
            lat, lon = get_latlon(f"{ward_name}, {province_name}")
            ward["lat"] = lat
            ward["lon"] = lon
            time.sleep(1)  
        time.sleep(2)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nHoàn tất! Dữ liệu depth=2 có tọa độ được lưu trong: {OUTPUT_FILE}")
    

