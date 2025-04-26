from elasticsearch import Elasticsearch
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time

# Khởi tạo kết nối Elasticsearch
es = Elasticsearch(['http://34.122.185.194:9200'])

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
    
    response = es.search(index=index_name, body=query)
    
    districts = []
    avg_prices = []
    
    for bucket in response['aggregations']['group_by_district']['buckets']:
        districts.append(bucket['key'])
        avg_prices.append(bucket['avg_price']['value'])
        
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Giá Trung Bình (VNĐ)': avg_prices
    })
    
    return df.sort_values('Giá Trung Bình (VNĐ)', ascending=False)

def get_price_per_square_by_district(estate_type="nhapho"):
    """
    Lấy dữ liệu giá trung bình trên mét vuông theo quận/huyện
    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
    Returns:
        DataFrame: DataFrame chứa thông tin quận/huyện và giá trung bình trên mét vuông
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
        "aggs": {
            "group_by_district": {
                "terms": {
                    "field": "address.district.keyword",
                    "size": 100
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
    
    districts = []
    avg_prices_per_square = []
    
    for bucket in response['aggregations']['group_by_district']['buckets']:
        districts.append(bucket['key'])
        avg_prices_per_square.append(bucket['avg_price_per_square']['value'])
        
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Giá Trung Bình/m² (VNĐ)': avg_prices_per_square
    })
    
    return df.sort_values('Giá Trung Bình/m² (VNĐ)', ascending=False)

def get_area_by_district(estate_type="nhapho"):
    """
    Lấy dữ liệu diện tích trung bình theo quận/huyện
    Args:
        estate_type (str): Loại nhà ('nhapho', 'nharieng', 'chungcu' hoặc 'bietthu')
    Returns:
        DataFrame: DataFrame chứa thông tin quận/huyện và diện tích trung bình
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
        "aggs": {
            "group_by_district": {
                "terms": {
                    "field": "address.district.keyword",
                    "size": 100
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
    
    districts = []
    avg_areas = []
    
    for bucket in response['aggregations']['group_by_district']['buckets']:
        districts.append(bucket['key'])
        avg_areas.append(bucket['avg_area']['value'])
        
    df = pd.DataFrame({
        'Quận/Huyện': districts,
        'Diện tích trung bình (m²)': avg_areas
    })
    
    return df.sort_values('Diện tích trung bình (m²)', ascending=False)


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

    df = pd.DataFrame({
        "Ngày": dates,
        "Giá Trung Bình (VNĐ)": avg_prices
    })

    return df.sort_values("Ngày", ascending=True)

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

    df = pd.DataFrame({
        "Ngày": dates,
        "Giá Trung Bình/m² (VNĐ)": avg_prices_per_square
    })

    return df.sort_values("Ngày", ascending=True)


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
    estate_type: str,
    is_latest_posted: bool = False,
    is_latest_created: bool = False,
    district: str = None,
    front_face: float = None,
    front_road: float = None,
    no_bathrooms: int = None,
    no_bedrooms: int = None,
    no_floors: int = None,
    ultilization_square: float = None,
    description: str = None
) -> List[Dict[str, Any]]:
    """
    Truy vấn bài đăng bất động sản với các tiêu chí linh hoạt.
    - is_latest_posted: Sắp xếp theo post_date giảm dần nếu True.
    - is_latest_created: Sắp xếp theo created_at giảm dần nếu True.
    - Mỗi nhóm tiêu chí (district, attr, description) trả về tối đa 3 bài.
    - Tổng tối đa 12 bài nếu tất cả nhóm đều được truy vấn.
    """
    index_mapping = {
        "nhà phố": "nhapho_index",
        "nhà riêng": "nharieng_index",
        "chung cư": "chungcu_index",
        "biệt thự": "bietthu_index"
    }
    INDEX_NAME = index_mapping[estate_type]
    results = []

    # Xác định sort nếu có
    sort = []
    if is_latest_posted:
        sort.append({"post_date": {"order": "desc"}})
    if is_latest_created:
        sort.append({"created_at": {"order": "desc"}})

    # 1. Truy vấn theo district cho get_latest_created_post
    if district is not None:
        query = {
            "query": {
                "match": {
                    "address.district": district
                }
            },
            "size": 3  # Lấy tối đa 3 bài
        }
        if sort:
            query["sort"] = sort
        try:
            response = es.search(index=INDEX_NAME, body=query)
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying district: {e}")

    # 2. Truy vấn theo district cho get_latest_post (nếu chưa truy vấn ở trên)
    if district is not None and not is_latest_created:
        query = {
            "query": {
                "match": {
                    "address.district": district
                }
            },
            "size": 3
        }
        if is_latest_posted:
            query["sort"] = [{"post_date": {"order": "desc"}}]
        try:
            response = es.search(index=INDEX_NAME, body=query)
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying district for post_date: {e}")

    # 3. Truy vấn theo extra_infos (get_post_by_attr)
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
                    "minimum_should_match": 1  # Thỏa mãn ít nhất 1 điều kiện
                }
            },
            "size": 3
        }
        if is_latest_posted:
            query["sort"] = [{"post_date": {"order": "desc"}}]
        try:
            response = es.search(index=INDEX_NAME, body=query)
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying extra_infos: {e}")

    # 4. Truy vấn theo description
    if description is not None:
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
            response = es.search(index=INDEX_NAME, body=query)
            results.extend([hit["_source"] for hit in response["hits"]["hits"][:3]])
        except Exception as e:
            print(f"Error querying description: {e}")

    # Loại bỏ trùng lặp và giới hạn tổng số kết quả
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