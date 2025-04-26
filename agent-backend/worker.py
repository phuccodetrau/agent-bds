from elasticsearch_queries import get_price_by_district, get_price_per_square_by_district, get_area_by_district, get_price_by_date, get_price_per_square_by_date
from manager import ResearchManager



def get_price_district(estate_type_index: str):
    return get_price_by_district(estate_type_index)

def get_price_per_square_district(estate_type_index: str):
    return get_price_per_square_by_district(estate_type_index)

def get_area_district(estate_type_index: str):
    return get_area_by_district(estate_type_index)

def get_price_date(estate_type_index: str, selected_district: str):
    return get_price_by_date(estate_type_index, selected_district)

def get_price_per_square_date(estate_type_index: str, selected_district: str):
    return get_price_per_square_by_date(estate_type_index, selected_district)

async def get_response(query):
    report = await ResearchManager().run(query)
    return report