import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from typing import Dict, Any, List, Optional


def create_cache_session(cache_file: str = '.cache', expire_after: int = 3600):
    """Tạo session với cache để lưu trữ dữ liệu tạm thời"""
    return requests_cache.CachedSession(cache_file, expire_after=expire_after)


def create_retry_session(session, retries: int = 5, backoff_factor: float = 0.2):
    """Tạo session với khả năng retry khi gặp lỗi"""
    return retry(session, retries=retries, backoff_factor=backoff_factor)


def create_openmeteo_client(session):
    """Tạo client Open-Meteo"""
    return openmeteo_requests.Client(session=session)


class WeatherAPI:
    def __init__(self, cache_file: str = '.cache', expire_after: int = 3600, 
                 retries: int = 5, backoff_factor: float = 0.2):
        """
        Khởi tạo WeatherAPI với các tùy chọn cấu hình
        
        Args:
            cache_file: Tên file cache
            expire_after: Thời gian hết hạn cache (giây)
            retries: Số lần thử lại khi gặp lỗi
            backoff_factor: Hệ số backoff cho retry
        """
        self.API_URL = "https://api.open-meteo.com/v1/forecast"
        self.cache_session = create_cache_session(cache_file, expire_after)
        self.retry_session = create_retry_session(self.cache_session, retries, backoff_factor)
        self.openmeteo = create_openmeteo_client(self.retry_session)

    def get_weather(self, params: Dict[str, Any]) -> List:
        """
        Lấy dữ liệu thời tiết từ API
        
        Args:
            params: Tham số truy vấn (latitude, longitude, current, etc.)
            
        Returns:
            List các response từ API
        """
        return self.openmeteo.weather_api(self.API_URL, params=params)

    def get_current_weather(self, latitude: float, longitude: float, 
                           variables: List[str] = None) -> Dict[str, Any]:
        """
        Lấy dữ liệu thời tiết hiện tại cho một vị trí
        
        Args:
            latitude: Vĩ độ
            longitude: Kinh độ
            variables: Danh sách các biến thời tiết cần lấy
            
        Returns:
            Dictionary chứa dữ liệu thời tiết hiện tại
        """
        if variables is None:
            variables = ["temperature_2m"]
            
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(variables)
        }
        
        responses = self.get_weather(params)
        return self._process_current_weather_response(responses[0], variables)

    def get_forecast_weather(self, latitude: float, longitude: float,
                            hourly_variables: List[str] = None,
                            daily_variables: List[str] = None,
                            forecast_days: int = 7) -> Dict[str, Any]:
        """
        Lấy dự báo thời tiết cho một vị trí
        
        Args:
            latitude: Vĩ độ
            longitude: Kinh độ
            hourly_variables: Danh sách biến theo giờ
            daily_variables: Danh sách biến theo ngày
            forecast_days: Số ngày dự báo
            
        Returns:
            Dictionary chứa dữ liệu dự báo
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_days": forecast_days
        }
        
        if hourly_variables:
            params["hourly"] = ",".join(hourly_variables)
        if daily_variables:
            params["daily"] = ",".join(daily_variables)
            
        responses = self.get_weather(params)
        return self._process_forecast_response(responses[0], hourly_variables, daily_variables)

    def _process_current_weather_response(self, response, variables: List[str]) -> Dict[str, Any]:
        """Xử lý response cho dữ liệu thời tiết hiện tại"""
        current = response.Current()
        result = {
            "coordinates": {
                "latitude": response.Latitude(),
                "longitude": response.Longitude()
            },
            "elevation": response.Elevation(),
            "timezone_offset": response.UtcOffsetSeconds(),
            "current_time": current.Time(),
            "current_data": {}
        }
        
        for i, variable in enumerate(variables):
            result["current_data"][variable] = current.Variables(i).Value()
            
        return result

    def _process_forecast_response(self, response, hourly_vars: List[str], 
                                 daily_vars: List[str]) -> Dict[str, Any]:
        """Xử lý response cho dữ liệu dự báo"""
        result = {
            "coordinates": {
                "latitude": response.Latitude(),
                "longitude": response.Longitude()
            },
            "elevation": response.Elevation(),
            "timezone_offset": response.UtcOffsetSeconds()
        }
        
        if hourly_vars:
            hourly = response.Hourly()
            result["hourly"] = self._extract_hourly_data(hourly, hourly_vars)
            
        if daily_vars:
            daily = response.Daily()
            result["daily"] = self._extract_daily_data(daily, daily_vars)
            
        return result

    def _extract_hourly_data(self, hourly, variables: List[str]) -> Dict[str, Any]:
        """Trích xuất dữ liệu theo giờ"""
        time_data = hourly.Time()
        # Chuyển đổi time data thành list nếu cần
        if hasattr(time_data, '__len__') and not isinstance(time_data, (int, float)):
            data = {"time": time_data}
        else:
            data = {"time": [time_data]}
            
        for i, variable in enumerate(variables):
            values = hourly.Variables(i).ValuesAsNumpy()
            data[variable] = values
        return data

    def _extract_daily_data(self, daily, variables: List[str]) -> Dict[str, Any]:
        """Trích xuất dữ liệu theo ngày"""
        time_data = daily.Time()
        # Chuyển đổi time data thành list nếu cần
        if hasattr(time_data, '__len__') and not isinstance(time_data, (int, float)):
            data = {"time": time_data}
        else:
            data = {"time": [time_data]}
            
        for i, variable in enumerate(variables):
            values = daily.Variables(i).ValuesAsNumpy()
            data[variable] = values
        return data

    def print_weather_info(self, weather_data: Dict[str, Any]):
        """In thông tin thời tiết ra console"""
        coords = weather_data["coordinates"]
        print(f"Coordinates: {coords['latitude']}°N {coords['longitude']}°E")
        print(f"Elevation: {weather_data['elevation']} m asl")
        print(f"Timezone difference to GMT+0: {weather_data['timezone_offset']}s")
        
        if "current_time" in weather_data:
            print(f"\nCurrent time: {weather_data['current_time']}")
            for var, value in weather_data["current_data"].items():
                print(f"Current {var}: {value}")

