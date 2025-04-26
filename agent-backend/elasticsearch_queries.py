from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import time

# Khởi tạo kết nối Elasticsearch
es = Elasticsearch(['http://34.171.201.34:9200'])
valid_districts = [
        "Ba Đình", "Bắc Từ Liêm", "Cầu Giấy", "Đống Đa", "Hà Đông", "Hai Bà Trưng",
        "Hoàn Kiếm", "Hoàng Mai", "Long Biên", "Nam Từ Liêm", "Tây Hồ", "Thanh Xuân",
        "Ba Vì", "Chương Mỹ", "Đan Phượng", "Đông Anh", "Gia Lâm", "Hoài Đức",
        "Mê Linh", "Mỹ Đức", "Phú Xuyên", "Phúc Thọ", "Quốc Oai", "Sóc Sơn",
        "Thạch Thất", "Thanh Oai", "Thanh Trì", "Thường Tín", "Ứng Hòa", "Sơn Tây"
    ]
def get_price_by_district(estate_type="nhapho"):
    """
    Lấy dữ liệu giá trung bình theo quận/huyện
    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
    Returns:
        DataFrame: DataFrame chứa thông tin quận/huyện và giá trung bình
    """
    index_mapping = {
        "nhapho": "nhapho_index",
        "nharieng": "nharieng_index",
        "chungcu": "chungcu_index",
        "bietthu": "bietthu_index"
    }
    
    index_name = index_mapping.get(estate_type, "nhapho_index")

    query = {
        "size": 0,
        "query": {
            "terms": {
                "address.district.keyword": valid_districts
            }
        },
        "aggs": {
            "group_by_district": {
                "terms": {
                    "field": "address.district.keyword",
                    "size": len(valid_districts)
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
    
    response = es.search(index=index_name, body=query)
    
    districts = valid_districts
    avg_prices = []

    avg_price_by_district = {district: 0 for district in valid_districts}

    # Cập nhật giá trị trung bình từ kết quả truy vấn
    for bucket in response['aggregations']['group_by_district']['buckets']:
        district = bucket['key']
        avg_price = bucket['avg_price']['value']
        if avg_price is not None:
            avg_price_by_district[district] = avg_price

    for district in districts:
        avg_prices.append(avg_price_by_district[district])

    return districts, avg_prices

        
    # df = pd.DataFrame({
    #     'Quận/Huyện': districts,
    #     'Giá Trung Bình (VNĐ)': avg_prices
    # })
    #
    # return df.sort_values('Giá Trung Bình (VNĐ)', ascending=False)


def get_price_per_square_by_district(estate_type="nhapho"):
    """
    Lấy dữ liệu giá trung bình trên mét vuông theo quận/huyện
    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
    Returns:
        tuple: (districts, avg_prices_per_square) chứa danh sách quận/huyện và giá trung bình trên mét vuông
    """

    index_mapping = {
        "nhapho": "nhapho_index",
        "nharieng": "nharieng_index",
        "chungcu": "chungcu_index",
        "bietthu": "bietthu_index"
    }

    index_name = index_mapping.get(estate_type, "nhapho_index")

    query = {
        "size": 0,
        "query": {
            "terms": {
                "address.district.keyword": valid_districts
            }
        },
        "aggs": {
            "group_by_district": {
                "terms": {
                    "field": "address.district.keyword",
                    "size": len(valid_districts)
                },
                "aggs": {
                    "avg_price_per_square": {
                        "avg": {
                            "field": "price/square"
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, body=query)

    districts = valid_districts
    avg_prices_per_square = []

    # Tạo dictionary để lưu giá trung bình theo quận/huyện, mặc định là 0
    avg_price_per_square_by_district = {district: 0 for district in valid_districts}

    # Cập nhật giá trị trung bình từ kết quả truy vấn
    for bucket in response['aggregations']['group_by_district']['buckets']:
        district = bucket['key']
        avg_price = bucket['avg_price_per_square']['value']
        if avg_price is not None:
            avg_price_per_square_by_district[district] = avg_price

    # Tạo danh sách giá trung bình theo thứ tự của valid_districts
    for district in districts:
        avg_prices_per_square.append(avg_price_per_square_by_district[district])

    return districts, avg_prices_per_square
        
    # df = pd.DataFrame({
    #     'Quận/Huyện': districts,
    #     'Giá Trung Bình/m² (VNĐ)': avg_prices_per_square
    # })
    #
    # return df.sort_values('Giá Trung Bình/m² (VNĐ)', ascending=False)


def get_area_by_district(estate_type="nhapho"):
    """
    Lấy dữ liệu diện tích trung bình theo quận/huyện
    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
    Returns:
        tuple: (districts, avg_areas) chứa danh sách quận/huyện và diện tích trung bình
    """
    valid_districts = [
        "Ba Đình", "Bắc Từ Liêm", "Cầu Giấy", "Đống Đa", "Hà Đông", "Hai Bà Trưng",
        "Hoàn Kiếm", "Hoàng Mai", "Long Biên", "Nam Từ Liêm", "Tây Hồ", "Thanh Xuân",
        "Ba Vì", "Chương Mỹ", "Đan Phượng", "Đông Anh", "Gia Lâm", "Hoài Đức",
        "Mê Linh", "Mỹ Đức", "Phú Xuyên", "Phúc Thọ", "Quốc Oai", "Sóc Sơn",
        "Thạch Thất", "Thanh Oai", "Thanh Trì", "Thường Tín", "Ứng Hòa", "Sơn Tây"
    ]

    index_mapping = {
        "nhapho": "nhapho_index",
        "nharieng": "nharieng_index",
        "chungcu": "chungcu_index",
        "bietthu": "bietthu_index"
    }

    index_name = index_mapping.get(estate_type, "nhapho_index")

    query = {
        "size": 0,
        "query": {
            "terms": {
                "address.district.keyword": valid_districts
            }
        },
        "aggs": {
            "group_by_district": {
                "terms": {
                    "field": "address.district.keyword",
                    "size": len(valid_districts)
                },
                "aggs": {
                    "avg_area": {
                        "avg": {
                            "field": "square"
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, body=query)

    districts = valid_districts
    avg_areas = []

    # Tạo dictionary để lưu diện tích trung bình theo quận/huyện, mặc định là 0
    avg_area_by_district = {district: 0 for district in valid_districts}

    # Cập nhật diện tích trung bình từ kết quả truy vấn
    for bucket in response['aggregations']['group_by_district']['buckets']:
        district = bucket['key']
        avg_area = bucket['avg_area']['value']
        if avg_area is not None:
            avg_area_by_district[district] = avg_area

    # Tạo danh sách diện tích trung bình theo thứ tự của valid_districts
    for district in districts:
        avg_areas.append(avg_area_by_district[district])

    return districts, avg_areas
        
    # df = pd.DataFrame({
    #     'Quận/Huyện': districts,
    #     'Diện tích trung bình (m²)': avg_areas
    # })
    #
    # return df.sort_values('Diện tích trung bình (m²)', ascending=False)


def get_price_by_date(estate_type, district):
    """
    Lấy giá trung bình theo từng ngày trong 10 ngày gần nhất cho một quận/huyện cụ thể.

    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
        district (str): Tên quận/huyện

    Returns:
        DataFrame: DataFrame chứa thông tin ngày và giá trung bình
    """
    index_mapping = {
        "nhapho": "nhapho_index",
        "nharieng": "nharieng_index",
        "chungcu": "chungcu_index",
        "bietthu": "bietthu_index"
    }

    index_name = index_mapping.get(estate_type, "nhapho_index")

    # Lấy ngày hiện tại và 10 ngày trước
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "address.district.keyword": district
                        }
                    },
                    {
                        "range": {
                            "price": {"gt": 0}  # Chỉ lấy bài đăng có giá trị hợp lệ
                        }
                    },
                    {
                        "range": {
                            "post_date": {
                                "gte": start_date.strftime("%Y/%m/%d"),
                                "lte": end_date.strftime("%Y/%m/%d"),
                                "format": "yyyy/MM/dd"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "price_by_date": {
                "date_histogram": {
                    "field": "post_date",
                    "calendar_interval": "day"
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

    response = es.search(index=index_name, body=query)

    if "aggregations" not in response:
        return pd.DataFrame(columns=["Ngày", "Giá Trung Bình (VNĐ)"])

    dates = []
    avg_prices = []

    for bucket in response["aggregations"]["price_by_date"]["buckets"]:
        dates.append(bucket["key_as_string"])
        avg_prices.append(bucket["avg_price"]["value"] or 0)

    return dates, avg_prices

    # df = pd.DataFrame({
    #     "Ngày": dates,
    #     "Giá Trung Bình (VNĐ)": avg_prices
    # })
    #
    # return df.sort_values("Ngày", ascending=True)

def get_price_per_square_by_date(estate_type, district):
    """
    Lấy giá trung bình/m² theo từng ngày trong 10 ngày gần nhất cho một quận/huyện cụ thể.

    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
        district (str): Tên quận/huyện

    Returns:
        DataFrame: DataFrame chứa thông tin ngày và giá trung bình/m²
    """
    index_mapping = {
        "nhapho": "nhapho_index",
        "nharieng": "nharieng_index",
        "chungcu": "chungcu_index",
        "bietthu": "bietthu_index"
    }

    index_name = index_mapping.get(estate_type, "nhapho_index")

    # Lấy ngày hiện tại và 10 ngày trước
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "address.district.keyword": district
                        }
                    },
                    {
                        "range": {
                            "price": {"gt": 0}  # Chỉ lấy bài đăng có giá trị hợp lệ
                        }
                    },
                    {
                        "range": {
                            "post_date": {
                                "gte": start_date.strftime("%Y/%m/%d"),
                                "lte": end_date.strftime("%Y/%m/%d"),
                                "format": "yyyy/MM/dd"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "price_by_date": {
                "date_histogram": {
                    "field": "post_date",
                    "calendar_interval": "day"
                },
                "aggs": {
                    "avg_price_per_square": {
                        "avg": {
                            "field": "price/square"
                        }
                    }
                }
            }
        }
    }

    response = es.search(index=index_name, body=query)

    if "aggregations" not in response:
        return pd.DataFrame(columns=["Ngày", "Giá Trung Bình/m² (VNĐ)"])

    dates = []
    avg_prices_per_square = []

    for bucket in response["aggregations"]["price_by_date"]["buckets"]:
        dates.append(bucket["key_as_string"])
        avg_prices_per_square.append(bucket["avg_price_per_square"]["value"] or 0)

    return  dates, avg_prices_per_square

    # df = pd.DataFrame({
    #     "Ngày": dates,
    #     "Giá Trung Bình/m² (VNĐ)": avg_prices_per_square
    # })
    #
    # return df.sort_values("Ngày", ascending=True)


def get_latest_created_post(index: str, district: str) -> Optional[Dict[str, Any]]:
    """
    Lấy bài đăng mới nhất theo thời gian created_at dựa trên quận (district).
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = index_mapping[index]
    query = {
        "query": {
            "match": {
                "address.district": district
            }
        },
        "sort": [
            {
                "created_at": {
                    "order": "desc"  # Sắp xếp giảm dần để lấy bài mới nhất
                }
            }
        ],
        "size": 1  # Chỉ lấy 1 kết quả
    }

    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        if hits:
            return hits[0]["_source"]
        return None
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return None


def get_latest_post(index: str, district: str) -> Optional[Dict[str, Any]]:
    """
    Lấy bài đăng mới nhất theo thời gian post_date dựa trên quận (district).
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = index_mapping[index]
    query = {
        "query": {
            "match": {
                "address.district": district
            }
        },
        "sort": [
            {
                "post_date": {
                    "order": "desc"  # Sắp xếp giảm dần để lấy bài mới nhất
                }
            }
        ],
        "size": 1  # Chỉ lấy 1 kết quả
    }

    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        if hits:
            return hits[0]["_source"]
        return None
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return None


def get_post_by_attr(
        index: str,
        front_face: float = None,
        front_road: float = None,
        no_bathrooms: int = None,
        no_bedrooms: int = None,
        no_floors: int = None,
        ultilization_square: float = None
) -> List[Dict[str, Any]]:
    """
    Lấy các bài đăng dựa trên các thuộc tính trong extra_infos.
    Chỉ sử dụng các tham số được cung cấp (khác None) trong truy vấn.
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = index_mapping[index]
    # Tạo danh sách các filter dựa trên các tham số được cung cấp
    filters = []

    if front_face is not None:
        filters.append({"term": {"extra_infos.front_face": front_face}})
    if front_road is not None:
        filters.append({"term": {"extra_infos.front_road": front_road}})
    if no_bathrooms is not None:
        filters.append({"term": {"extra_infos.no_bathrooms": no_bathrooms}})
    if no_bedrooms is not None:
        filters.append({"term": {"extra_infos.no_bedrooms": no_bedrooms}})
    if no_floors is not None:
        filters.append({"term": {"extra_infos.no_floors": no_floors}})
    if ultilization_square is not None:
        filters.append({"term": {"extra_infos.ultilization_square": ultilization_square}})

    # Nếu không có tham số nào được cung cấp, trả về empty list
    if not filters:
        return []

    # Tạo query với các filter
    query = {
        "query": {
            "bool": {
                "filter": filters
            }
        }
    }

    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        return [hit["_source"] for hit in hits]
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return []


def get_post_by_description(index: str, des: str) -> List[Dict[str, Any]]:
    """
    Lấy các bài đăng dựa trên tìm kiếm từ khóa trong trường description.
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = index_mapping[index]
    query = {
        "query": {
            "match": {
                "description": {
                    "query": des,
                    "fuzziness": "AUTO"  # Cho phép tìm kiếm gần đúng
                }
            }
        }
    }

    try:
        response = es.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        return [hit["_source"] for hit in hits]
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return []


def search_posts(
    estate_type: List[str],
    is_latest_posted: bool = None,
    is_latest_created: bool = None,
    district: List[str] = None,
    front_face: float = None,
    front_road: float = None,
    no_bathrooms: int = None,
    no_bedrooms: int = None,
    no_floors: int = None,
    ultilization_square: float = None,
    price: float = None,
    price_per_square: float = None,
    square: float = None,
    description: str = None
) -> List[Dict[str, Any]]:
    """
    Truy vấn bài đăng bất động sản với các tiêu chí linh hoạt, trả về kết quả ngẫu nhiên.
    - estate_type: Danh sách các loại bất động sản.
    - district: Danh sách các quận/huyện.
    - is_latest_posted: Sắp xếp theo post_date giảm dần nếu True.
    - is_latest_created: Sắp xếp theo created_at giảm dần nếu True.
    - Mỗi nhóm tiêu chí trả về tối đa 3 bài, tổng tối đa 12 bài.
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = ",".join([index_mapping[et] for et in estate_type if et in index_mapping])
    results = []

    sort = []
    if is_latest_posted:
        sort.append({"post_date": {"order": "desc"}})
    if is_latest_created:
        sort.append({"created_at": {"order": "desc"}})

    def wrap_with_random_score(query: dict) -> dict:
        return {
            "query": {
                "function_score": {
                    "query": query["query"],
                    "random_score": {
                        "field": "_seq_no"
                    }
                }
            },
            "size": query.get("size", 3)
        }

    if district:
        query = {
            "query": {
                "terms": {
                    "address.district.keyword": district
                }
            },
            "size": 3
        }
        if sort:
            query["sort"] = sort
        try:
            response = es.search(index=INDEX_NAME, body=wrap_with_random_score(query))
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying district: {e}")

    if district and not is_latest_created:
        query = {
            "query": {
                "terms": {
                    "address.district.keyword": district
                }
            },
            "size": 3
        }
        if is_latest_posted:
            query["sort"] = [{"post_date": {"order": "desc"}}]
        try:
            response = es.search(index=INDEX_NAME, body=wrap_with_random_score(query))
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying district for post_date: {e}")

    attr_params = {
        "front_face": front_face,
        "front_road": front_road,
        "no_bathrooms": no_bathrooms,
        "no_bedrooms": no_bedrooms,
        "no_floors": no_floors,
        "ultilization_square": ultilization_square
    }
    if any(v is not None for v in attr_params.values()):
        should_clauses = [
            {"term": {f"extra_infos.{k}": v}}
            for k, v in attr_params.items() if v is not None
        ]
        query = {
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            },
            "size": 3
        }
        if is_latest_posted:
            query["sort"] = [{"post_date": {"order": "desc"}}]
        try:
            response = es.search(index=INDEX_NAME, body=wrap_with_random_score(query))
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying extra_infos: {e}")

    range_params = {
        "price": price,
        "price/square": price_per_square,
        "square": square
    }
    range_clauses = []
    for field, val in range_params.items():
        if val is not None:
            delta = val * 0.1
            range_clauses.append({
                "range": {
                    field: {
                        "gte": val - delta,
                        "lte": val + delta
                    }
                }
            })
    if range_clauses:
        query = {
            "query": {
                "bool": {
                    "should": range_clauses,
                    "minimum_should_match": 1
                }
            },
            "size": 3
        }
        if sort:
            query["sort"] = sort
        try:
            response = es.search(index=INDEX_NAME, body=wrap_with_random_score(query))
            results.extend([hit["_source"] for hit in response["hits"]["hits"]])
        except Exception as e:
            print(f"Error querying price-related fields: {e}")

    if description:
        query = {
            "query": {
                "match": {
                    "description": {
                        "query": description,
                        "fuzziness": "AUTO"
                    }
                }
            },
            "size": 3
        }
        if is_latest_posted:
            query["sort"] = [{"post_date": {"order": "desc"}}]
        try:
            response = es.search(index=INDEX_NAME, body=wrap_with_random_score(query))
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying description: {e}")

    seen_ids = set()
    unique_results = []
    for result in results:
        post_id = result.get("post_id")
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_results.append(result)
            if len(unique_results) >= 12:
                break

    return unique_results

def search_posts_strict(
    estate_type: List[str],
    is_latest_posted: bool = None,
    is_latest_created: bool = None,
    district: List[str] = None,
    front_face: float = None,
    front_road: float = None,
    no_bathrooms: int = None,
    no_bedrooms: int = None,
    no_floors: int = None,
    ultilization_square: float = None,
    price: float = None,
    price_per_square: float = None,
    square: float = None,
    description: str = None
) -> List[Dict[str, Any]]:
    """
    Truy vấn bài đăng bất động sản với các tiêu chí nghiêm ngặt, trả về kết quả ngẫu nhiên.
    - Tất cả tiêu chí phải được thỏa mãn.
    - Hỗ trợ nhiều estate_type và district.
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = ",".join([index_mapping[et] for et in estate_type if et in index_mapping])
    must_clauses = []
    must_clauses.append({"terms": {"estate_type": estate_type}})

    if district:
        must_clauses.append({"terms": {"address.district.keyword": district}})

    extra_fields = {
        "front_face": front_face,
        "front_road": front_road,
        "no_bathrooms": no_bathrooms,
        "no_bedrooms": no_bedrooms,
        "no_floors": no_floors,
        "ultilization_square": ultilization_square
    }
    for key, value in extra_fields.items():
        if value is not None:
            must_clauses.append({"term": {f"extra_infos.{key}": value}})

    range_fields = {
        "price": price,
        "price/square": price_per_square,
        "square": square
    }
    for field, val in range_fields.items():
        if val is not None:
            delta = val * 0.1
            must_clauses.append({
                "range": {
                    field: {
                        "gte": val - delta,
                        "lte": val + delta
                    }
                }
            })

    if description:
        must_clauses.append({
            "match": {
                "description": {
                    "query": description,
                    "fuzziness": "AUTO"
                }
            }
        })

    sort = []
    if is_latest_posted:
        sort.append({"post_date": {"order": "desc"}})
    if is_latest_created:
        sort.append({"created_at": {"order": "desc"}})

    query_body = {
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "random_score": {
                    "field": "_seq_no"
                }
            }
        },
        "size": 12
    }
    if sort:
        query_body["sort"] = sort

    try:
        response = es.search(index=INDEX_NAME, body=query_body)
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return []

if __name__ == "__main__":
    result = search_posts(estate_type="chung cư", is_latest_posted=True, district="Đống Đa", price_per_square=60000000.0)
    print(json.dumps(result, indent=4, ensure_ascii=False))


# def search_posts(
#     estate_type: str,
#     is_latest_posted: bool = None,
#     is_latest_created: bool = None,
#     district: str = None,
#     front_face: float = None,
#     front_road: float = None,
#     no_bathrooms: int = None,
#     no_bedrooms: int = None,
#     no_floors: int = None,
#     ultilization_square: float = None,
#     price: float = None,
#     price_per_square = None,
#     square = None,
#     description: str = None
# ) -> List[Dict[str, Any]]:
#     """
#     Truy vấn bài đăng bất động sản với các tiêu chí linh hoạt.
#     - is_latest_posted: Sắp xếp theo post_date giảm dần nếu True.
#     - is_latest_created: Sắp xếp theo created_at giảm dần nếu True.
#     - Mỗi nhóm tiêu chí (district, attr, description) trả về tối đa 3 bài.
#     - Tổng tối đa 12 bài nếu tất cả nhóm đều được truy vấn.
#     """
#     index_mapping = {
#         "nhà phố": "nhapho_index",
#         "nhà riêng": "nharieng_index",
#         "chung cư": "chungcu_index",
#         "biệt thự": "bietthu_index"
#     }
#     INDEX_NAME = index_mapping[estate_type]
#     results = []
#
#     # Xác định sort nếu có
#     sort = []
#     if is_latest_posted:
#         sort.append({"post_date": {"order": "desc"}})
#     if is_latest_created:
#         sort.append({"created_at": {"order": "desc"}})
#
#     # 1. Truy vấn theo district cho get_latest_created_post
#     if district is not None:
#         query = {
#             "query": {
#                 "match": {
#                     "address.district": district
#                 }
#             },
#             "size": 3  # Lấy tối đa 3 bài
#         }
#         if sort:
#             query["sort"] = sort
#         try:
#             response = es.search(index=INDEX_NAME, body=query)
#             results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
#         except Exception as e:
#             print(f"Error querying district: {e}")
#
#     # 2. Truy vấn theo district cho get_latest_post (nếu chưa truy vấn ở trên)
#     if district is not None and not is_latest_created:
#         query = {
#             "query": {
#                 "match": {
#                     "address.district": district
#                 }
#             },
#             "size": 3
#         }
#         if is_latest_posted:
#             query["sort"] = [{"post_date": {"order": "desc"}}]
#         try:
#             response = es.search(index=INDEX_NAME, body=query)
#             results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
#         except Exception as e:
#             print(f"Error querying district for post_date: {e}")
#
#     # 3. Truy vấn theo extra_infos (get_post_by_attr)
#     attr_params = {
#         "front_face": front_face,
#         "front_road": front_road,
#         "no_bathrooms": no_bathrooms,
#         "no_bedrooms": no_bedrooms,
#         "no_floors": no_floors,
#         "ultilization_square": ultilization_square
#     }
#     if any(v is not None for v in attr_params.values()):
#         should_clauses = [
#             {"term": {f"extra_infos.{k}": v}}
#             for k, v in attr_params.items() if v is not None
#         ]
#         query = {
#             "query": {
#                 "bool": {
#                     "should": should_clauses,
#                     "minimum_should_match": 1  # Thỏa mãn ít nhất 1 điều kiện
#                 }
#             },
#             "size": 3
#         }
#         if is_latest_posted:
#             query["sort"] = [{"post_date": {"order": "desc"}}]
#         try:
#             response = es.search(index=INDEX_NAME, body=query)
#             results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
#         except Exception as e:
#             print(f"Error querying extra_infos: {e}")
#     range_params = {
#         "price": price,
#         "price/square": price_per_square,
#         "square": square
#     }
#     range_clauses = []
#     for field, val in range_params.items():
#         if val is not None:
#             delta = val * 0.1
#             range_clauses.append({
#                 "range": {
#                     field: {
#                         "gte": val - delta,
#                         "lte": val + delta
#                     }
#                 }
#             })
#     if range_clauses:
#         query = {
#             "query": {
#                 "bool": {
#                     "should": range_clauses,
#                     "minimum_should_match": 1
#                 }
#             },
#             "size": 3
#         }
#         if sort:
#             query["sort"] = sort
#         try:
#             response = es.search(index=INDEX_NAME, body=query)
#             results.extend([hit["_source"] for hit in response["hits"]["hits"]])
#         except Exception as e:
#             print(f"Error querying price-related fields: {e}")
#
#     # 4. Truy vấn theo description
#     if description is not None:
#         query = {
#             "query": {
#                 "match": {
#                     "description": {
#                         "query": description,
#                         "fuzziness": "AUTO"
#                     }
#                 }
#             },
#             "size": 3
#         }
#         if is_latest_posted:
#             query["sort"] = [{"post_date": {"order": "desc"}}]
#         try:
#             response = es.search(index=INDEX_NAME, body=query)
#             results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
#         except Exception as e:
#             print(f"Error querying description: {e}")
#
#     # Loại bỏ trùng lặp và giới hạn tổng số kết quả
#     seen_ids = set()
#     unique_results = []
#     for result in results:
#         post_id = result.get("post_id")
#         if post_id and post_id not in seen_ids:
#             seen_ids.add(post_id)
#             unique_results.append(result)
#             if len(unique_results) >= 12:
#                 break
#
#     return unique_results
#
#
# def search_posts_strict(
#         estate_type: str,
#         is_latest_posted: bool = None,
#         is_latest_created: bool = None,
#         district: str = None,
#         front_face: float = None,
#         front_road: float = None,
#         no_bathrooms: int = None,
#         no_bedrooms: int = None,
#         no_floors: int = None,
#         ultilization_square: float = None,
#         price: float = None,
#         price_per_square: float = None,
#         square: float = None,
#         description: str = None
# ) -> List[Dict[str, Any]]:
#     index_mapping = {
#         "nhà phố": "nhapho_index",
#         "nhà riêng": "nharieng_index",
#         "chung cư": "chungcu_index",
#         "biệt thự": "bietthu_index"
#     }
#     INDEX_NAME = index_mapping[estate_type]
#     must_clauses = []
#
#     # 1. Match district
#     if district:
#         must_clauses.append({"match": {"address.district": district}})
#
#     # 2. Match extra_infos
#     extra_fields = {
#         "front_face": front_face,
#         "front_road": front_road,
#         "no_bathrooms": no_bathrooms,
#         "no_bedrooms": no_bedrooms,
#         "no_floors": no_floors,
#         "ultilization_square": ultilization_square
#     }
#     for key, value in extra_fields.items():
#         if value is not None:
#             must_clauses.append({"term": {f"extra_infos.{key}": value}})
#
#     # 3. Range cho giá cả, diện tích...
#     range_fields = {
#         "price": price,
#         "price/square": price_per_square,
#         "square": square
#     }
#     for field, val in range_fields.items():
#         if val is not None:
#             delta = val * 0.1
#             must_clauses.append({
#                 "range": {
#                     field: {
#                         "gte": val - delta,
#                         "lte": val + delta
#                     }
#                 }
#             })
#
#     # 4. Fuzzy match description
#     if description:
#         must_clauses.append({
#             "match": {
#                 "description": {
#                     "query": description,
#                     "fuzziness": "AUTO"
#                 }
#             }
#         })
#
#     # 5. Sắp xếp
#     sort = []
#     if is_latest_posted:
#         sort.append({"post_date": {"order": "desc"}})
#     if is_latest_created:
#         sort.append({"created_at": {"order": "desc"}})
#
#     # 6. Tạo query và gọi Elasticsearch
#     query_body = {
#         "query": {
#             "bool": {
#                 "must": must_clauses
#             }
#         },
#         "size": 12
#     }
#     if sort:
#         query_body["sort"] = sort
#
#     try:
#         response = es.search(index=INDEX_NAME, body=query_body)
#         return [hit["_source"] for hit in response["hits"]["hits"]]
#     except Exception as e:
#         print(f"Error querying Elasticsearch: {e}")
#         return []