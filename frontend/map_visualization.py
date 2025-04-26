import folium
from folium import plugins
from geopy.geocoders import Nominatim
from elasticsearch_queries import get_price_by_district, get_price_per_square_by_district
import pandas as pd
import numpy as np

def get_district_coordinates(district):
    """
    Lấy tọa độ của quận/huyện từ tên
    """
    geolocator = Nominatim(user_agent="my_agent")
    location = geolocator.geocode(f"{district}, Hà Nội")
    if location:
        return [location.latitude, location.longitude]
    return None

def create_price_heatmap(estate_type="nhapho"):
    """
    Tạo bản đồ heatmap thể hiện giá trung bình theo quận/huyện
    """
    # Lấy dữ liệu giá
    price_df = get_price_by_district(estate_type)
    
    # Tạo bản đồ Hà Nội
    m = folium.Map(location=[21.0285, 105.8542], zoom_start=11)
    
    # Chuẩn hóa giá để tạo màu
    max_price = price_df['Giá Trung Bình (VNĐ)'].max()
    min_price = price_df['Giá Trung Bình (VNĐ)'].min()
    
    # Thêm các điểm heatmap
    heat_data = []
    for _, row in price_df.iterrows():
        coords = get_district_coordinates(row['Quận/Huyện'])
        if coords:
            # Chuẩn hóa giá từ 0-1
            normalized_price = (row['Giá Trung Bình (VNĐ)'] - min_price) / (max_price - min_price)
            heat_data.append(coords + [normalized_price])
    
    # Thêm heatmap layer
    plugins.HeatMap(heat_data).add_to(m)
    
    return m

def create_price_per_square_heatmap(estate_type="nhapho"):
    """
    Tạo bản đồ heatmap thể hiện giá trung bình/m² theo quận/huyện
    """
    # Lấy dữ liệu giá/m²
    price_per_square_df = get_price_per_square_by_district(estate_type)
    
    # Tạo bản đồ Hà Nội
    m = folium.Map(location=[21.0285, 105.8542], zoom_start=11)
    
    # Chuẩn hóa giá để tạo màu
    max_price = price_per_square_df['Giá Trung Bình/m² (VNĐ)'].max()
    min_price = price_per_square_df['Giá Trung Bình/m² (VNĐ)'].min()
    
    # Thêm các điểm heatmap
    heat_data = []
    for _, row in price_per_square_df.iterrows():
        coords = get_district_coordinates(row['Quận/Huyện'])
        if coords:
            # Chuẩn hóa giá từ 0-1
            normalized_price = (row['Giá Trung Bình/m² (VNĐ)'] - min_price) / (max_price - min_price)
            heat_data.append(coords + [normalized_price])
    
    # Thêm heatmap layer
    plugins.HeatMap(heat_data).add_to(m)
    
    return m 