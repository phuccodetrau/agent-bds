import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from elasticsearch_queries import get_price_by_district, get_price_per_square_by_district, get_area_by_district, \
    get_price_by_date, get_price_per_square_by_date
from map_visualization import create_price_heatmap, create_price_per_square_heatmap
import requests
from typing import List, Dict
import asyncio
from typing import Any
import json

BASE_URL = "http://localhost:8000"


# Hàm chạy async trong môi trường đồng bộ
def run_async(coroutine):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # Nếu vòng lặp đã chạy (trong Streamlit), dùng cách khác để chạy
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result()
    else:
        return loop.run_until_complete(coroutine)


# Các hàm gọi API được chuyển sang đồng bộ
def price_by_date(estate_type_index: str, district: str):
    endpoint = f"{BASE_URL}/get_price_by_date/{estate_type_index}/{district}"
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    dates, avg_prices = data["dates"], data["avg_prices"]
    df = pd.DataFrame({
        "Ngày": dates,
        "Giá Trung Bình (VNĐ)": avg_prices
    })
    return df.sort_values("Ngày", ascending=True)


def price_per_square_by_date(estate_type_index: str, district: str):
    endpoint = f"{BASE_URL}/get_price_per_square_by_date/{estate_type_index}/{district}"
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    dates, avg_prices_per_square = data["dates"], data["avg_prices_per_square"]
    df = pd.DataFrame({
        "Ngày": dates,
        "Giá Trung Bình/m² (VNĐ)": avg_prices_per_square
    })
    return df.sort_values("Ngày", ascending=True)


def price_by_district(estate_type_index: str):
    endpoint = f"{BASE_URL}/get_price_by_district/{estate_type_index}"
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    districts, avg_prices = data["districts"], data["avg_prices"]
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Giá Trung Bình (VNĐ)': avg_prices
    })
    return df.sort_values('Giá Trung Bình (VNĐ)', ascending=False)


def price_per_square_by_district(estate_type_index: str):
    endpoint = f"{BASE_URL}/get_price_per_square_by_district/{estate_type_index}"
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    districts, avg_prices_per_square = data["districts"], data["avg_prices_per_square"]
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Giá Trung Bình/m² (VNĐ)': avg_prices_per_square
    })
    return df.sort_values('Giá Trung Bình/m² (VNĐ)', ascending=False)


def area_by_district(estate_type_index: str):
    endpoint = f"{BASE_URL}/get_area_by_district/{estate_type_index}"
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    districts, avg_areas = data["districts"], data["avg_areas"]
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Diện tích trung bình (m²)': avg_areas
    })
    return df.sort_values('Diện tích trung bình (m²)', ascending=False)


def get_response(messages):
    endpoint = f"{BASE_URL}/chat/"
    # Chuyển messages (list) thành JSON string để gửi qua API
    response = requests.post(endpoint, data=json.dumps(messages), headers={"Content-Type": "application/json"})
    return response.json()


# Thiết lập cấu hình trang
st.set_page_config(page_title="Chatbot & Dashboard App", layout="wide")

# Tạo sidebar để chuyển đổi giữa các tab
st.sidebar.title("Điều hướng")
page = st.sidebar.radio("Chọn trang", ["Chatbot", "Dashboard"])

if page == "Chatbot":
    st.title("🤖 Chatbot")

    # Khởi tạo lịch sử chat trong session state
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Thêm nút New Chat
    if st.button("🔄 New Chat"):
        st.session_state.messages = []
        st.rerun()

    # Hiển thị lịch sử chat
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Nhập tin nhắn từ người dùng
    if prompt := st.chat_input("Nhập tin nhắn của bạn..."):
        # Thêm tin nhắn của người dùng vào lịch sử
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Gọi API với toàn bộ lịch sử tin nhắn
        response = get_response(st.session_state.messages)

        # Phản hồi từ chatbot
        with st.chat_message("assistant"):
            st.markdown(response["real_estate_findings"] + "\n")
            st.markdown("Phân tích: \n" + response["analytics_and_advice"] + "\n")
            relevant_question = "\n".join(response["follow_up_questions"])
            st.markdown("Câu hỏi có thể bạn quan tâm: \n" + relevant_question + "\n")
            st.session_state.messages.append({"role": "assistant", "content": response["real_estate_findings"] + "\n" + response["analytics_and_advice"]})
else:
    st.title("📊 Dashboard")

    # Thêm nút chọn loại nhà
    estate_type = st.radio(
        "Chọn loại nhà:",
        ["Nhà phố", "Nhà riêng", "Chung cư", "Biệt thự"],
        horizontal=True
    )

    # Chuyển đổi tên hiển thị sang tên index
    estate_type_mapping = {
        "Nhà phố": "nhapho",
        "Nhà riêng": "nharieng",
        "Chung cư": "chungcu",
        "Biệt thự": "bietthu"
    }
    estate_type_index = estate_type_mapping.get(estate_type, "nhapho")

    # Lấy dữ liệu giá trung bình theo quận/huyện
    try:
        price_by_district_df = price_by_district(estate_type_index)
        price_per_square_df = price_per_square_by_district(estate_type_index)
        area_by_district_df = area_by_district(estate_type_index)

        # Tạo layout 3 cột
        col1, col2, col3 = st.columns(3)

        with col1:
            # Biểu đồ giá trung bình theo quận/huyện
            fig_price_district = px.bar(price_by_district_df,
                                        x='Quận/Huyện',
                                        y='Giá Trung Bình (VNĐ)',
                                        title=f'Giá Trung Bình {estate_type} Theo Quận/Huyện Hà Nội',
                                        labels={'Quận/Huyện': 'Quận/Huyện',
                                                'Giá Trung Bình (VNĐ)': 'Giá Trung Bình (VNĐ)'})

            # Tùy chỉnh giao diện biểu đồ
            fig_price_district.update_layout(
                xaxis_tickangle=-45,
                showlegend=False,
                height=400,
                margin=dict(t=50, b=100)
            )

            st.plotly_chart(fig_price_district, use_container_width=True)

            # Hiển thị bảng dữ liệu giá trung bình
            st.subheader(f"Bảng giá trung bình theo quận/huyện ({estate_type})")
            st.dataframe(price_by_district_df, use_container_width=True)

        with col2:
            # Biểu đồ giá trung bình trên mét vuông
            fig_price_per_square = px.bar(price_per_square_df,
                                          x='Quận/Huyện',
                                          y='Giá Trung Bình/m² (VNĐ)',
                                          title=f'Giá Trung Bình/m² {estate_type} Theo Quận/Huyện Hà Nội',
                                          labels={'Quận/Huyện': 'Quận/Huyện',
                                                  'Giá Trung Bình/m² (VNĐ)': 'Giá Trung Bình/m² (VNĐ)'})

            # Tùy chỉnh giao diện biểu đồ
            fig_price_per_square.update_layout(
                xaxis_tickangle=-45,
                showlegend=False,
                height=400,
                margin=dict(t=50, b=100)
            )

            st.plotly_chart(fig_price_per_square, use_container_width=True)

            # Hiển thị bảng dữ liệu giá trung bình/m²
            st.subheader(f"Bảng giá trung bình/m² theo quận/huyện ({estate_type})")
            st.dataframe(price_per_square_df, use_container_width=True)

        with col3:
            # Biểu đồ diện tích trung bình theo quận/huyện
            fig_area_district = px.bar(area_by_district_df,
                                       x='Quận/Huyện',
                                       y='Diện tích trung bình (m²)',
                                       title=f'Diện Tích Trung Bình {estate_type} Theo Quận/Huyện Hà Nội',
                                       labels={'Quận/Huyện': 'Quận/Huyện',
                                               'Diện tích trung bình (m²)': 'Diện tích trung bình (m²)'})

            # Tùy chỉnh giao diện biểu đồ
            fig_area_district.update_layout(
                xaxis_tickangle=-45,
                showlegend=False,
                height=400,
                margin=dict(t=50, b=100)
            )

            st.plotly_chart(fig_area_district, use_container_width=True)

            # Hiển thị bảng dữ liệu diện tích trung bình
            st.subheader(f"Bảng diện tích trung bình theo quận/huyện ({estate_type})")
            st.dataframe(area_by_district_df, use_container_width=True)

        # Thêm biểu đồ theo thời gian
        st.markdown("---")
        st.subheader("Biểu đồ giá theo thời gian")

        # Tạo dropdown chọn quận/huyện
        selected_district = st.selectbox(
            "Chọn quận/huyện:",
            options=price_by_district_df['Quận/Huyện'].tolist()
        )

        # Tạo layout 2 cột cho biểu đồ theo thời gian
        time_col1, time_col2 = st.columns(2)

        with time_col1:
            # Lấy dữ liệu giá theo thời gian cho quận/huyện được chọn
            price_by_date_df = price_by_date(estate_type_index, selected_district)

            # Lọc dữ liệu để chỉ giữ các ngày có giá trung bình khác 0
            price_by_date_df_filtered = price_by_date_df[price_by_date_df['Giá Trung Bình (VNĐ)'] != 0]

            # Vẽ biểu đồ line chart giá
            fig_price_trend = px.line(
                price_by_date_df_filtered,
                x='Ngày',
                y='Giá Trung Bình (VNĐ)',
                title=f'Giá Trung Bình {estate_type} Theo Thời Gian - {selected_district}',
                labels={'Ngày': 'Ngày',
                        'Giá Trung Bình (VNĐ)': 'Giá Trung Bình (VNĐ)'}
            )

            # Tùy chỉnh giao diện biểu đồ
            fig_price_trend.update_layout(
                xaxis_tickangle=-45,
                showlegend=False,
                height=400,
                margin=dict(t=50, b=100)
            )

            st.plotly_chart(fig_price_trend, use_container_width=True)

            # Hiển thị bảng dữ liệu giá (dữ liệu đã lọc)
            st.subheader(f"Bảng giá theo thời gian - {selected_district}")
            st.dataframe(price_by_date_df_filtered, use_container_width=True)

        with time_col2:
            # Lấy dữ liệu giá/m² theo thời gian cho quận/huyện được chọn
            price_per_square_by_date_df = price_per_square_by_date(estate_type_index, selected_district)

            # Lọc dữ liệu để chỉ giữ các ngày có giá trung bình/m² khác 0
            price_per_square_by_date_df_filtered = price_per_square_by_date_df[
                price_per_square_by_date_df['Giá Trung Bình/m² (VNĐ)'] != 0]

            # Vẽ biểu đồ line chart giá/m²
            fig_price_per_square_trend = px.line(
                price_per_square_by_date_df_filtered,
                x='Ngày',
                y='Giá Trung Bình/m² (VNĐ)',
                title=f'Giá Trung Bình/m² {estate_type} Theo Thời Gian - {selected_district}',
                labels={'Ngày': 'Ngày',
                        'Giá Trung Bình/m² (VNĐ)': 'Giá Trung Bình/m² (VNĐ)'}
            )

            # Tùy chỉnh giao diện biểu đồ
            fig_price_per_square_trend.update_layout(
                xaxis_tickangle=-45,
                showlegend=False,
                height=400,
                margin=dict(t=50, b=100)
            )

            st.plotly_chart(fig_price_per_square_trend, use_container_width=True)

            # Hiển thị bảng dữ liệu giá/m² (dữ liệu đã lọc)
            st.subheader(f"Bảng giá/m² theo thời gian - {selected_district}")
            st.dataframe(price_per_square_by_date_df_filtered, use_container_width=True)

        # Thêm bản đồ heatmap
        st.markdown("---")
        st.subheader("Bản đồ nhiệt giá theo quận/huyện")

        # Tạo layout 2 cột cho bản đồ
        map_col1, map_col2 = st.columns(2)

        with map_col1:
            st.subheader("Bản đồ nhiệt giá trung bình")
            price_map = create_price_heatmap(estate_type_index)
            st.components.v1.html(price_map._repr_html_(), height=500)

        with map_col2:
            st.subheader("Bản đồ nhiệt giá trung bình/m²")
            price_per_square_map = create_price_per_square_heatmap(estate_type_index)
            st.components.v1.html(price_per_square_map._repr_html_(), height=500)

    except Exception as e:
        st.error(f"Có lỗi xảy ra khi kết nối đến Elasticsearch: {str(e)}")
        st.info("Vui lòng kiểm tra kết nối và thử lại sau.")

if __name__ == "__main__":
    # Đảm bảo chạy ứng dụng trong môi trường chính xác
    st.write("Ứng dụng đang chạy...")