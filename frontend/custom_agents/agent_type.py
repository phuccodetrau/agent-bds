from typing import Optional
from pydantic import BaseModel

class Address(BaseModel):
    district: str
    full_address: str
    province: str
    ward: str

class ContactInfo(BaseModel):
    name: str
    phone: str

class ExtraInfos(BaseModel):
    direction: Optional[str] = None
    front_face: Optional[float] = None
    front_road: Optional[float] = None
    no_bathrooms: Optional[int] = None
    no_bedrooms: Optional[int] = None
    no_floors: Optional[int] = None
    ultilization_square: Optional[float] = None
    yo_construction: Optional[int] = None

class Post(BaseModel):
    address: Address
    contact_info: ContactInfo
    description: str
    estate_type: str
    extra_infos: ExtraInfos
    id: int
    link: str
    post_date: str  # Có thể dùng datetime.datetime nếu bạn muốn parse date
    created_at: str
    post_id: str
    price: float
    price_per_square: float  # Chuyển "price/square" thành tên biến hợp lệ
    square: float
    title: str

    class Config:
        # Cho phép gán giá trị từ dictionary với tên field không khớp chính xác
        allow_population_by_field_name = True
        # Xử lý các field tên không hợp lệ trong Python (như "price/square")
        alias_generator = lambda field_name: field_name.replace("_per_", "/")
