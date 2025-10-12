"""
Utility functions for weather data processing and analysis
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta


def convert_temperature(celsius: float, to_unit: str = "fahrenheit") -> float:
    """
    Chuyển đổi nhiệt độ từ Celsius sang các đơn vị khác
    
    Args:
        celsius: Nhiệt độ theo Celsius
        to_unit: Đơn vị đích ("fahrenheit", "kelvin")
        
    Returns:
        Nhiệt độ đã chuyển đổi
    """
    if to_unit.lower() == "fahrenheit":
        return (celsius * 9/5) + 32
    elif to_unit.lower() == "kelvin":
        return celsius + 273.15
    else:
        return celsius


def format_weather_data(weather_data: Dict[str, Any], 
                       temperature_unit: str = "celsius") -> Dict[str, Any]:
    """
    Format dữ liệu thời tiết để hiển thị đẹp hơn
    
    Args:
        weather_data: Dữ liệu thời tiết thô
        temperature_unit: Đơn vị nhiệt độ muốn hiển thị
        
    Returns:
        Dữ liệu đã được format
    """
    formatted_data = weather_data.copy()
    
    # Format thời gian
    if "current_time" in formatted_data:
        formatted_data["current_time"] = datetime.fromisoformat(
            formatted_data["current_time"].replace("Z", "+00:00")
        )
    
    # Format nhiệt độ
    if "current_data" in formatted_data:
        for key, value in formatted_data["current_data"].items():
            if "temperature" in key and temperature_unit != "celsius":
                formatted_data["current_data"][key] = convert_temperature(value, temperature_unit)
    
    return formatted_data


def calculate_weather_statistics(weather_data_list: List[Dict[str, Any]], 
                               variable: str = "temperature_2m") -> Dict[str, float]:
    """
    Tính toán thống kê cho một biến thời tiết từ nhiều vị trí
    
    Args:
        weather_data_list: Danh sách dữ liệu thời tiết
        variable: Biến cần tính thống kê
        
    Returns:
        Dictionary chứa các thống kê (min, max, avg, median)
    """
    values = []
    for data in weather_data_list:
        if "current_data" in data and variable in data["current_data"]:
            values.append(data["current_data"][variable])
    
    if not values:
        return {"min": 0, "max": 0, "avg": 0, "median": 0}
    
    values.sort()
    return {
        "min": min(values),
        "max": max(values),
        "avg": sum(values) / len(values),
        "median": values[len(values) // 2]
    }


def create_weather_dataframe(weather_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Tạo DataFrame từ danh sách dữ liệu thời tiết
    
    Args:
        weather_data_list: Danh sách dữ liệu thời tiết
        
    Returns:
        pandas DataFrame
    """
    rows = []
    for data in weather_data_list:
        row = {
            "location": data.get("location_name", "Unknown"),
            "latitude": data["coordinates"]["latitude"],
            "longitude": data["coordinates"]["longitude"],
            "elevation": data["elevation"],
            "timezone_offset": data["timezone_offset"]
        }
        
        if "current_data" in data:
            row.update(data["current_data"])
        
        rows.append(row)
    
    return pd.DataFrame(rows)


def save_weather_data_to_csv(weather_data_list: List[Dict[str, Any]], 
                           filename: str = "weather_data.csv"):
    """
    Lưu dữ liệu thời tiết ra file CSV
    
    Args:
        weather_data_list: Danh sách dữ liệu thời tiết
        filename: Tên file CSV
    """
    df = create_weather_dataframe(weather_data_list)
    df.to_csv(filename, index=False)
    print(f"Weather data saved to {filename}")


def load_weather_data_from_csv(filename: str) -> pd.DataFrame:
    """
    Đọc dữ liệu thời tiết từ file CSV
    
    Args:
        filename: Tên file CSV
        
    Returns:
        pandas DataFrame
    """
    return pd.read_csv(filename)


def get_weather_alerts(weather_data: Dict[str, Any]) -> List[str]:
    """
    Phân tích dữ liệu thời tiết và đưa ra cảnh báo
    
    Args:
        weather_data: Dữ liệu thời tiết
        
    Returns:
        Danh sách các cảnh báo
    """
    alerts = []
    
    if "current_data" in weather_data:
        current_data = weather_data["current_data"]
        
        # Cảnh báo nhiệt độ
        if "temperature_2m" in current_data:
            temp = current_data["temperature_2m"]
            if temp > 35:
                alerts.append("⚠️ Nhiệt độ cao - Cảnh báo nắng nóng")
            elif temp < 0:
                alerts.append("❄️ Nhiệt độ thấp - Cảnh báo lạnh")
        
        # Cảnh báo độ ẩm
        if "relative_humidity_2m" in current_data:
            humidity = current_data["relative_humidity_2m"]
            if humidity > 80:
                alerts.append("💧 Độ ẩm cao - Có thể có mưa")
            elif humidity < 30:
                alerts.append("🌵 Độ ẩm thấp - Thời tiết khô")
        
        # Cảnh báo mưa
        if "precipitation" in current_data:
            precipitation = current_data["precipitation"]
            if precipitation > 5:
                alerts.append("🌧️ Mưa lớn - Cảnh báo mưa")
    
    return alerts


def compare_weather_locations(weather_data_list: List[Dict[str, Any]], 
                            variable: str = "temperature_2m") -> Dict[str, Any]:
    """
    So sánh thời tiết giữa các vị trí
    
    Args:
        weather_data_list: Danh sách dữ liệu thời tiết
        variable: Biến cần so sánh
        
    Returns:
        Dictionary chứa kết quả so sánh
    """
    comparison = {
        "variable": variable,
        "locations": [],
        "statistics": calculate_weather_statistics(weather_data_list, variable)
    }
    
    for data in weather_data_list:
        location_name = data.get("location_name", "Unknown")
        if "current_data" in data and variable in data["current_data"]:
            value = data["current_data"][variable]
            comparison["locations"].append({
                "name": location_name,
                "value": value,
                "coordinates": data["coordinates"]
            })
    
    # Sắp xếp theo giá trị
    comparison["locations"].sort(key=lambda x: x["value"], reverse=True)
    
    return comparison


def print_weather_comparison(comparison: Dict[str, Any]):
    """
    In kết quả so sánh thời tiết
    
    Args:
        comparison: Kết quả so sánh từ compare_weather_locations
    """
    print(f"\n=== So sánh {comparison['variable']} ===")
    
    stats = comparison["statistics"]
    print(f"Thống kê:")
    print(f"  - Nhiệt độ thấp nhất: {stats['min']:.1f}°C")
    print(f"  - Nhiệt độ cao nhất: {stats['max']:.1f}°C")
    print(f"  - Nhiệt độ trung bình: {stats['avg']:.1f}°C")
    print(f"  - Nhiệt độ trung vị: {stats['median']:.1f}°C")
    
    print(f"\nXếp hạng theo {comparison['variable']}:")
    for i, location in enumerate(comparison["locations"], 1):
        print(f"  {i}. {location['name']}: {location['value']:.1f}°C")
