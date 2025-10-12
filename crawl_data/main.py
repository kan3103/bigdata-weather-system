from weatherAPI import WeatherAPI
from weather_utils import (
    format_weather_data, 
    calculate_weather_statistics, 
    create_weather_dataframe,
    save_weather_data_to_csv,
    get_weather_alerts,
    compare_weather_locations,
    print_weather_comparison
)
from typing import List, Dict, Any


def get_weather_for_location(api: WeatherAPI, latitude: float, longitude: float, 
                           variables: List[str] = None) -> Dict[str, Any]:
    """
    Lấy dữ liệu thời tiết cho một vị trí cụ thể
    
    Args:
        api: Instance của WeatherAPI
        latitude: Vĩ độ
        longitude: Kinh độ
        variables: Danh sách các biến thời tiết cần lấy
        
    Returns:
        Dictionary chứa dữ liệu thời tiết
    """
    if variables is None:
        variables = ["temperature_2m"]
    
    return api.get_current_weather(latitude, longitude, variables)


def get_forecast_for_location(api: WeatherAPI, latitude: float, longitude: float,
                            hourly_vars: List[str] = None, daily_vars: List[str] = None,
                            forecast_days: int = 7) -> Dict[str, Any]:
    """
    Lấy dự báo thời tiết cho một vị trí cụ thể
    
    Args:
        api: Instance của WeatherAPI
        latitude: Vĩ độ
        longitude: Kinh độ
        hourly_vars: Biến theo giờ
        daily_vars: Biến theo ngày
        forecast_days: Số ngày dự báo
        
    Returns:
        Dictionary chứa dữ liệu dự báo
    """
    return api.get_forecast_weather(latitude, longitude, hourly_vars, daily_vars, forecast_days)


def get_weather_for_multiple_locations(api: WeatherAPI, locations: List[Dict[str, Any]], 
                                     variables: List[str] = None) -> List[Dict[str, Any]]:
    """
    Lấy dữ liệu thời tiết cho nhiều vị trí
    
    Args:
        api: Instance của WeatherAPI
        locations: Danh sách các vị trí với latitude và longitude
        variables: Danh sách các biến thời tiết
        
    Returns:
        List các dictionary chứa dữ liệu thời tiết
    """
    results = []
    for location in locations:
        weather_data = get_weather_for_location(
            api, 
            location["latitude"], 
            location["longitude"], 
            variables
        )
        weather_data["location_name"] = location.get("name", "Unknown")
        results.append(weather_data)
    
    return results


def print_weather_summary(weather_data: Dict[str, Any]):
    """
    In tóm tắt thông tin thời tiết
    
    Args:
        weather_data: Dictionary chứa dữ liệu thời tiết
    """
    location_name = weather_data.get("location_name", "Unknown Location")
    print(f"\n=== Weather for {location_name} ===")
    
    coords = weather_data["coordinates"]
    print(f"Coordinates: {coords['latitude']}°N {coords['longitude']}°E")
    print(f"Elevation: {weather_data['elevation']} m asl")
    print(f"Timezone difference to GMT+0: {weather_data['timezone_offset']}s")
    
    if "current_time" in weather_data:
        print(f"\nCurrent time: {weather_data['current_time']}")
        print("Current weather conditions:")
        for var, value in weather_data["current_data"].items():
            print(f"  - {var}: {value}")
    
    # Hiển thị cảnh báo thời tiết
    alerts = get_weather_alerts(weather_data)
    if alerts:
        print("\nWeather Alerts:")
        for alert in alerts:
            print(f"  {alert}")


def demonstrate_weather_analysis(weather_data_list: List[Dict[str, Any]]):
    """
    Demo các tính năng phân tích dữ liệu thời tiết
    
    Args:
        weather_data_list: Danh sách dữ liệu thời tiết
    """
    print("\n=== Weather Data Analysis ===")
    
    # Tạo DataFrame
    df = create_weather_dataframe(weather_data_list)
    print(f"\nDataFrame shape: {df.shape}")
    print("DataFrame columns:", list(df.columns))
    
    # Tính thống kê nhiệt độ
    temp_stats = calculate_weather_statistics(weather_data_list, "temperature_2m")
    print(f"\nTemperature Statistics:")
    print(f"  - Min: {temp_stats['min']:.1f}°C")
    print(f"  - Max: {temp_stats['max']:.1f}°C")
    print(f"  - Average: {temp_stats['avg']:.1f}°C")
    print(f"  - Median: {temp_stats['median']:.1f}°C")
    
    # So sánh các vị trí
    comparison = compare_weather_locations(weather_data_list, "temperature_2m")
    print_weather_comparison(comparison)
    
    # Lưu dữ liệu ra CSV
    save_weather_data_to_csv(weather_data_list, "weather_analysis.csv")


def main():
    """Hàm chính để chạy chương trình"""
    # Khởi tạo WeatherAPI
    weather_api = WeatherAPI()
    
    # Ví dụ 1: Lấy thời tiết hiện tại cho Berlin (tọa độ cũ)
    print("=== Example 1: Current Weather for Berlin ===")
    berlin_weather = get_weather_for_location(
        weather_api, 
        latitude=52.52, 
        longitude=13.41,
        variables=["temperature_2m", "relative_humidity_2m", "apparent_temperature"]
    )
    print_weather_summary(berlin_weather)
    
    # Ví dụ 2: Lấy dự báo thời tiết
    print("\n=== Example 2: Weather Forecast ===")
    forecast = get_forecast_for_location(
        weather_api,
        latitude=52.52,
        longitude=13.41,
        hourly_vars=["temperature_2m", "precipitation"],
        daily_vars=["temperature_2m_max", "temperature_2m_min"],
        forecast_days=3
    )
    
    print(f"Forecast for {forecast['coordinates']['latitude']}°N {forecast['coordinates']['longitude']}°E")
    if "hourly" in forecast:
        time_data = forecast['hourly']['time']
        if hasattr(time_data, '__len__'):
            print(f"Hourly data available for {len(time_data)} hours")
        else:
            print(f"Hourly data available: {time_data}")
    if "daily" in forecast:
        time_data = forecast['daily']['time']
        if hasattr(time_data, '__len__'):
            print(f"Daily data available for {len(time_data)} days")
        else:
            print(f"Daily data available: {time_data}")
    
    # Ví dụ 3: Lấy thời tiết cho nhiều thành phố
    print("\n=== Example 3: Multiple Cities ===")
    cities = [
        {"name": "Berlin", "latitude": 52.52, "longitude": 13.41},
        {"name": "Paris", "latitude": 48.85, "longitude": 2.35},
        {"name": "London", "latitude": 51.51, "longitude": -0.13}
    ]
    
    cities_weather = get_weather_for_multiple_locations(
        weather_api, 
        cities, 
        variables=["temperature_2m", "relative_humidity_2m", "apparent_temperature"]
    )
    
    for city_weather in cities_weather:
        print_weather_summary(city_weather)
    
    # Demo phân tích dữ liệu
    demonstrate_weather_analysis(cities_weather)


if __name__ == "__main__":
    main()