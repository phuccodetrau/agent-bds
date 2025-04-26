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
load_dotenv()

BASE_URL = os.getenv("EXAMPLE_BASE_URL") or ""
API_KEY = os.getenv("EXAMPLE_API_KEY") or ""
MODEL_NAME = os.getenv("EXAMPLE_MODEL_NAME") or ""

if not BASE_URL or not API_KEY or not MODEL_NAME:
    raise ValueError(
        "Please set EXAMPLE_BASE_URL, EXAMPLE_API_KEY, EXAMPLE_MODEL_NAME via env var or code."
    )


"""This example uses a custom provider for all requests by default. We do three things:
1. Create a custom client.
2. Set it as the default OpenAI client, and don't use it for tracing.
3. Set the default API as Chat Completions, as most LLM providers don't yet support Responses API.

Note that in this example, we disable tracing under the assumption that you don't have an API key
from platform.openai.com. If you do have one, you can either set the `OPENAI_API_KEY` env var
or call set_tracing_export_api_key() to set a tracing specific key.
"""

client = AsyncOpenAI(
    base_url=BASE_URL,
    api_key=API_KEY,
)
set_default_openai_client(client=client, use_for_tracing=False)
set_default_openai_api("chat_completions")
set_tracing_disabled(disabled=True)

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
    model=MODEL_NAME,
)
async def main():
    result = await Runner.run(planner_agent, input="Cho tôi các bài đăng nhà phố mới nhất tại quận Hoàn Kiếm có 3 tầng. Cho tôi thông tin về diện tích trung bình của các căn hộ Eco Park")
    print(result.final_output)
    list_tool = json.loads(result.final_output.replace("```", "").replace("json", ""))
    print(list_tool[0])
    print(list_tool[1])

if __name__ == "__main__":
    asyncio.run(main())