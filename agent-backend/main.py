from  fastapi import FastAPI, Body
from worker import get_area_district, get_price_district, get_price_per_square_district, get_price_date, get_price_per_square_date, get_response
from typing import Any

app = FastAPI()

@app.get("/get_price_by_district/{estate_type_index}")
def get_price_by_district(estate_type_index: str):
    districts, avg_prices = get_price_district(estate_type_index)
    return {"districts": districts, "avg_prices": avg_prices}

@app.get("/get_price_per_square_by_district/{estate_type_index}")
def get_price_per_square_by_district(estate_type_index: str):
    districts, avg_prices_per_square = get_price_per_square_district(estate_type_index)
    return {"districts": districts, "avg_prices_per_square": avg_prices_per_square}

@app.get("/get_area_by_district/{estate_type_index}")
def get_area_by_district(estate_type_index: str):
    districts, avg_areas = get_area_district(estate_type_index)
    return {"districts": districts, "avg_areas": avg_areas}

@app.get("/get_price_by_date/{estate_type_index}/{selected_district}")
def get_price_by_date(estate_type_index: str, selected_district: str):
    dates, avg_prices = get_price_date(estate_type_index, selected_district)
    return {"dates": dates, "avg_prices": avg_prices}

@app.get("/get_price_per_square_by_date/{estate_type_index}/{selected_district}")
def get_price_per_square_by_date(estate_type_index: str, selected_district: str):
    dates, avg_prices_per_square = get_price_per_square_date(estate_type_index, selected_district)
    return {"dates": dates, "avg_prices_per_square": avg_prices_per_square}

@app.post("/chat/")
async def chat(messages: Any = Body(...)):
    report = await get_response(messages)
    return {"real_estate_findings": report.real_estate_findings, "analytics_and_advice": report.analytics_and_advice, "follow_up_questions": report.follow_up_questions}
