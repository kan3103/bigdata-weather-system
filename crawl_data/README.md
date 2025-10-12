# Weather API System - Refactored

Hệ thống đã được tái cấu trúc thành các module riêng biệt để dễ quản lý và mở rộng.

## Cấu trúc Files

### 1. `weatherAPI.py` - Core API Class
- **Class WeatherAPI**: Class chính để tương tác với Open-Meteo API
- **Utility Functions**:
  - `create_cache_session()`: Tạo session với cache
  - `create_retry_session()`: Tạo session với retry mechanism
  - `create_openmeteo_client()`: Tạo Open-Meteo client

**Các phương thức chính:**
- `get_weather()`: Lấy dữ liệu thời tiết cơ bản
- `get_current_weather()`: Lấy thời tiết hiện tại
- `get_forecast_weather()`: Lấy dự báo thời tiết
- `print_weather_info()`: In thông tin thời tiết

### 2. `weather_utils.py` - Utility Functions
**Các hàm xử lý dữ liệu:**
- `convert_temperature()`: Chuyển đổi đơn vị nhiệt độ
- `format_weather_data()`: Format dữ liệu để hiển thị
- `calculate_weather_statistics()`: Tính thống kê
- `create_weather_dataframe()`: Tạo pandas DataFrame
- `save_weather_data_to_csv()`: Lưu dữ liệu ra CSV
- `get_weather_alerts()`: Phân tích và đưa ra cảnh báo
- `compare_weather_locations()`: So sánh thời tiết giữa các vị trí

### 3. `main.py` - Main Application
**Các hàm chính:**
- `get_weather_for_location()`: Lấy thời tiết cho một vị trí
- `get_forecast_for_location()`: Lấy dự báo cho một vị trí
- `get_weather_for_multiple_locations()`: Lấy thời tiết cho nhiều vị trí
- `print_weather_summary()`: In tóm tắt thông tin
- `demonstrate_weather_analysis()`: Demo phân tích dữ liệu
- `main()`: Hàm chính chạy chương trình

## Cách sử dụng

### 1. Lấy thời tiết hiện tại cho một vị trí:
```python
from weatherAPI import WeatherAPI

api = WeatherAPI()
weather = api.get_current_weather(
    latitude=52.52, 
    longitude=13.41,
    variables=["temperature_2m", "humidity", "pressure"]
)
```

### 2. Lấy dự báo thời tiết:
```python
forecast = api.get_forecast_weather(
    latitude=52.52,
    longitude=13.41,
    hourly_vars=["temperature_2m", "precipitation"],
    daily_vars=["temperature_2m_max", "temperature_2m_min"],
    forecast_days=7
)
```

### 3. Lấy thời tiết cho nhiều thành phố:
```python
from main import get_weather_for_multiple_locations

cities = [
    {"name": "Berlin", "latitude": 52.52, "longitude": 13.41},
    {"name": "Paris", "latitude": 48.85, "longitude": 2.35}
]

weather_data = get_weather_for_multiple_locations(api, cities)
```

### 4. Phân tích và so sánh dữ liệu:
```python
from weather_utils import (
    calculate_weather_statistics,
    compare_weather_locations,
    save_weather_data_to_csv
)

# Tính thống kê
stats = calculate_weather_statistics(weather_data, "temperature_2m")

# So sánh các vị trí
comparison = compare_weather_locations(weather_data, "temperature_2m")

# Lưu ra CSV
save_weather_data_to_csv(weather_data, "weather_data.csv")
```

## Chạy chương trình

```bash
# Kích hoạt virtual environment
source venv/bin/activate

# Chạy chương trình chính
python main.py
```

## Lợi ích của việc tái cấu trúc

1. **Modularity**: Code được chia thành các module riêng biệt
2. **Reusability**: Các hàm có thể tái sử dụng dễ dàng
3. **Maintainability**: Dễ bảo trì và debug
4. **Extensibility**: Dễ dàng thêm tính năng mới
5. **Type Safety**: Sử dụng type hints để tăng độ an toàn
6. **Documentation**: Mỗi hàm đều có docstring chi tiết

## Các tính năng mới

- ✅ Cache và retry mechanism
- ✅ Xử lý lỗi tốt hơn
- ✅ Phân tích dữ liệu thời tiết
- ✅ Cảnh báo thời tiết tự động
- ✅ So sánh thời tiết giữa các vị trí
- ✅ Xuất dữ liệu ra CSV
- ✅ Chuyển đổi đơn vị nhiệt độ
- ✅ Tạo pandas DataFrame
