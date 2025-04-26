from elasticsearch import Elasticsearch
import plotly.express as px
import pandas as pd

# Kết nối đến Elasticsearch
es = Elasticsearch(['http://34.136.244.2:9200'])

# Truy vấn dữ liệu từ Elasticsearch
query = {
    "size": 0,
    "aggs": {
        "group_by_district": {
            "terms": {
                "field": "address.district.keyword",
                "size": 100
            },
            "aggs": {
                "avg_price": {
                    "avg": {
                        "field": "price"
                    }
                }
            }
        }
    }
}

# Thực hiện truy vấn
response = es.search(index="nhapho_index", body=query)

# Xử lý dữ liệu
districts = []
avg_prices = []

for bucket in response['aggregations']['group_by_district']['buckets']:
    districts.append(bucket['key'])
    avg_prices.append(bucket['avg_price']['value'])

# Tạo DataFrame
df = pd.DataFrame({
    'Quận/Huyện': districts,
    'Giá Trung Bình (triệu VNĐ)': avg_prices
})

# Sắp xếp theo giá trung bình giảm dần
df = df.sort_values('Giá Trung Bình (triệu VNĐ)', ascending=False)

# Vẽ biểu đồ
fig = px.bar(df, 
             x='Quận/Huyện', 
             y='Giá Trung Bình (triệu VNĐ)',
             title='Giá Trung Bình Nhà Phố Theo Quận/Huyện Hà Nội',
             labels={'Quận/Huyện': 'Quận/Huyện', 'Giá Trung Bình (triệu VNĐ)': 'Giá Trung Bình (triệu VNĐ)'})

# Tùy chỉnh giao diện biểu đồ
fig.update_layout(
    xaxis_tickangle=-45,
    showlegend=False,
    height=600,
    margin=dict(t=50, b=100)
)

# Lưu biểu đồ
fig.write_html("price_by_district.html")

# In kết quả
print("\nKết quả giá trung bình theo quận/huyện:")
print(df.to_string(index=False)) 