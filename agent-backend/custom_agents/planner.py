from __future__ import annotations
import asyncio
from typing import Optional, List, Any, Literal
from pydantic import BaseModel
from elasticsearch_queries import search_posts  # Giả định đây là module chứa hàm search_posts
from openai import AsyncOpenAI
from dotenv import load_dotenv
from agents import (
    Agent,
    Runner,
    function_tool,
    set_default_openai_api,
    set_default_openai_client,
    set_tracing_disabled,
    ToolsToFinalOutputFunction,
    ToolsToFinalOutputResult,
    FunctionToolResult,
    ModelSettings,
    RunContextWrapper,
    TResponseInputItem,
    ItemHelpers,
    FunctionTool,
    AsyncOpenAI,
    OpenAIChatCompletionsModel,
    ModelSettings
)
from agents.run import RunConfig
from agents.items import ToolCallOutputItem
import os
import json


PROMPT = """You are a helpful real estate assistant. Given a user query in Vietnamese, choose from 2 tools based on their descriptions:
            - search_db: Retrieves real estate listings from the database relevant to the query.
            - search_web: Gathers general real estate information, such as price forecasts or project updates, based on the query.
            Select the appropriate tool(s) and return them as a list. Always choose at least 1 tool. Response with the list only. Do not output anything else especially ```json
            Examples:
            Input: Cho tôi các bài đăng chung cư mới nhất tại quận Thanh Xuân?
            Output: ["search_db"]
            
            Input: Tình hình dự án Vinhomes Smart City như thế nào rồi?
            Output: ["search_web"]
            
            Input: Lấy cho tôi các bài đăng nhà riêng có 2 phòng ngủ tại quận Thanh Xuân. Dự đoán giá nhà riêng tại khu vực Thanh Xuân trong 6 tháng tới.
            Output: ["search_db", "search_web"]
"""

planner_agent = Agent(
    name="PlannerAgent",
    instructions=PROMPT,
    model="gpt-4o-mini",
)
async def main():
    result = await Runner.run(planner_agent, input="Cho tôi các bài đăng nhà phố mới nhất tại quận Hoàn Kiếm có 3 tầng. Cho tôi thông tin về diện tích trung bình của các căn hộ Eco Park")
    print(result.final_output)
    list_tool = json.loads(result.final_output.replace("```", "").replace("json", ""))
    print(list_tool[0])
    print(list_tool[1])

if __name__ == "__main__":
    asyncio.run(main())