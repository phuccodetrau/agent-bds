from typing import List, Optional, Literal
from pydantic import BaseModel, Field

class EvaluationFeedback(BaseModel):
    feedback: str
    score: Literal["pass", "needs_improvement"]

class FunctionArgs(BaseModel):
    estate_type: List[Literal["nhà phố", "nhà riêng", "chung cư", "biệt thự"]] = Field(
        ..., description="Danh sách các loại bất động sản"
    )
    is_latest_posted: Optional[bool] = Field(description="Lấy bài đăng (căn, nhà) mới nhất")
    is_latest_created: Optional[bool] = Field(description="Lấy bài tạo mới nhất trong cơ sở dữ liệu")
    district: List[Literal[
        "Ba Đình", "Bắc Từ Liêm", "Cầu Giấy", "Đống Đa", "Hà Đông", "Hai Bà Trưng",
        "Hoàn Kiếm", "Hoàng Mai", "Long Biên", "Nam Từ Liêm", "Tây Hồ", "Thanh Xuân",
        "Ba Vì", "Chương Mỹ", "Đan Phượng", "Đông Anh", "Gia Lâm", "Hoài Đức",
        "Mê Linh", "Mỹ Đức", "Phú Xuyên", "Phúc Thọ", "Quốc Oai", "Sóc Sơn",
        "Thạch Thất", "Thanh Oai", "Thanh Trì", "Thường Tín", "Ứng Hòa", "Sơn Tây"
    ]] = Field(description="Danh sách các Quận/Huyện")
    front_face: Optional[float] = Field(description="Mặt tiền (m)")
    front_road: Optional[float] = Field(description="Đường trước nhà (m)")
    no_bathrooms: Optional[int] = Field(description="Số phòng tắm")
    no_bedrooms: Optional[int] = Field(description="Số phòng ngủ")
    no_floors: Optional[int] = Field(description="Số tầng")
    ultilization_square: Optional[float] = Field(description="Diện tích sử dụng (m²)")
    price: Optional[float] = Field(description="Giá")
    price_per_square: Optional[float] = Field(description="Giá trên mét vuông")
    square: Optional[float] = Field(description="Diện tích (m²)")
    description: Optional[str] = Field(description="Mô tả bất động sản")

    class Config:
        validate_by_name = True

class Address(BaseModel):
    district: str
    full_address: str
    province: str
    ward: str

class ContactInfo(BaseModel):
    name: str
    phone: List[str]

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
    post_date: str
    created_at: str
    post_id: str
    price: float
    price_per_square: float
    square: float
    title: str

    class Config:
        alias_generator = lambda field_name: field_name.replace("_per_", "/")
        populate_by_name = True

class ListPosts(BaseModel):
    posts: List[Post]