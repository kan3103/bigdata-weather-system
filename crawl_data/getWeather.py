import requests
import json
import time
from datetime import datetime

addresses = [
    "Quận 1, TP.HCM",
    "Quận 3, TP.HCM",
    "Quận 4, TP.HCM",
    "Quận 5, TP.HCM",
    "Quận 7, TP.HCM",
    "Quận 12, TP.HCM",
    "Quận Tân Bình, TP.HCM",
    "Huyện Củ Chi, TP.HCM",
    "Quận 6, TP.HCM",
    "Quận 8, TP.HCM",
    "Quận Bình Thạnh, TP.HCM",
    "Quận Tân Phú, TP.HCM",
    "Huyện Bình Chánh, TP.HCM",
    "Quận Gò Vấp, TP.HCM",
    "Quận Bình Tân, TP.HCM",
    "Huyện Cần Giờ, TP.HCM",
    "TP Thủ Đức, TP.HCM",
    "Quận 11, TP.HCM",
    "Quận Phú Nhuận, TP.HCM",
    "Huyện Hóc Môn, TP.HCM",
    "Huyện Nhà Bè, TP.HCM",
    "Quận 10, TP.HCM"
]

def get_latlon(address: str):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address + ", Hồ Chí Minh, Việt Nam",
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

def get_weather(lat: float, lon: float):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": True
    }
    r = requests.get(url, params=params)
    data = r.json()
    return data.get("current_weather", {})

def run_loop():
    locations  = {"timestamps": datetime.now().isoformat(), "addresses":[]}
    for addr in addresses:
        lat, lon = get_latlon(addr)
        locations["addresses"].append({"addr":addr, "lat":lat, "lon":lon, "weathers" : {}})

    while True:
        timestamp = datetime.now().isoformat()
        print(f"\n Lấy dữ liệu lúc {timestamp}")
        locations["timestamps"] = timestamp
        for addr in locations["addresses"]:
            weather = get_weather(addr["lat"], addr["lon"])
            addr["weathers"] = weather
            print(f"Lấy được thời tiết {addr}: {weather}")
        with open("data/weather_data.json", "w", encoding="utf-8") as f:
            json.dump(locations, f, ensure_ascii=False, indent=4)
        time.sleep(15)

if __name__ == "__main__":
    run_loop()