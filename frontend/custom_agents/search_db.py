from __future__ import annotations
import asyncio
from typing import Optional, List, Any, Literal
from pydantic import BaseModel, Field
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
load_dotenv()
#
# BASE_URL = os.getenv("EXAMPLE_BASE_URL") or ""
# API_KEY = os.getenv("EXAMPLE_API_KEY") or ""
# MODEL_NAME = os.getenv("EXAMPLE_MODEL_NAME") or ""
#
# if not BASE_URL or not API_KEY or not MODEL_NAME:
#     raise ValueError(
#         "Please set EXAMPLE_BASE_URL, EXAMPLE_API_KEY, EXAMPLE_MODEL_NAME via env var or code."
#     )
#
#
# """This example uses a custom provider for all requests by default. We do three things:
# 1. Create a custom client.
# 2. Set it as the default OpenAI client, and don't use it for tracing.
# 3. Set the default API as Chat Completions, as most LLM providers don't yet support Responses API.
#
# Note that in this example, we disable tracing under the assumption that you don't have an API key
# from platform.openai.com. If you do have one, you can either set the `OPENAI_API_KEY` env var
# or call set_tracing_export_api_key() to set a tracing specific key.
# """
#
# client = AsyncOpenAI(
#     base_url=BASE_URL,
#     api_key=API_KEY,
# )
# set_default_openai_client(client=client, use_for_tracing=False)
# set_default_openai_api("chat_completions")
# set_tracing_disabled(disabled=True)


class FunctionArgs(BaseModel):
    estate_type: Literal["nhà phố", "nhà riêng", "chung cư", "biệt thự"] = Field(..., description="Loại bất động sản")
    is_latest_posted: bool = Field(False, description="Lấy bài đăng(căn, nhà) mới nhất")
    is_latest_created: bool = Field(False, description="Lấy bài tạo mới nhất trong cơ sở dữ liệu")
    district: Optional[str] = Field(None, description="Quận/Huyện")
    front_face: Optional[float] = Field(None, description="Mặt tiền (m)")
    front_road: Optional[float] = Field(None, description="Đường trước nhà (m)")
    no_bathrooms: Optional[int] = Field(None, description="Số phòng tắm")
    no_bedrooms: Optional[int] = Field(None, description="Số phòng ngủ")
    no_floors: Optional[int] = Field(None, description="Số tầng")
    ultilization_square: Optional[float] = Field(None, description="Diện tích sử dụng (m²)")
    description: Optional[str] = Field(None, description="Mô tả bất động sản")

    class Config:
        validate_by_name = True

class Address(BaseModel):
    district: str
    full_address: str
    province: str
    ward: str


class ContactInfo(BaseModel):
    name: str
    phone: list[str]


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
        populate_by_name = True  # đúng cú pháp v2
class ListPosts(BaseModel):
    posts: list[Post]
async def run_function(ctx: RunContextWrapper[Any], args: str) -> List[Post]:
    # Parse tham số từ JSON thành FunctionArgs
    parsed = FunctionArgs.model_validate_json(args)
    print(parsed)

    # Gọi hàm search_posts với các tham số riêng lẻ từ parsed
    search_results = search_posts(
        estate_type=parsed.estate_type,  # Extract the index field
        is_latest_posted=parsed.is_latest_posted,
        is_latest_created=parsed.is_latest_created,
        district=parsed.district,
        front_face=parsed.front_face,
        front_road=parsed.front_road,
        no_bathrooms=parsed.no_bathrooms,
        no_bedrooms=parsed.no_bedrooms,
        no_floors=parsed.no_floors,
        ultilization_square=parsed.ultilization_square,
        description=parsed.description
    )

    # Chuyển đổi kết quả từ search_posts thành danh sách các đối tượng Post
    posts = []
    with open("tool_call.txt", "w", encoding="utf-8") as f:
        f.write(str(search_results))
    for result in search_results:
        try:
            # Ánh xạ các trường từ kết quả Elasticsearch vào Post
            post = Post(
                address=Address(
                    district=result["address"]["district"],
                    full_address=result["address"]["full_address"],
                    province=result["address"]["province"],
                    ward=result["address"]["ward"]
                ),
                contact_info=ContactInfo(
                    name=result["contact_info"]["name"],
                    phone=result["contact_info"]["phone"]
                ),
                description=result["description"],
                estate_type=result["estate_type"],
                extra_infos=ExtraInfos(
                    direction=result["extra_infos"].get("direction"),
                    front_face=result["extra_infos"].get("front_face"),
                    front_road=result["extra_infos"].get("front_road"),
                    no_bathrooms=result["extra_infos"].get("no_bathrooms"),
                    no_bedrooms=result["extra_infos"].get("no_bedrooms"),
                    no_floors=result["extra_infos"].get("no_floors"),
                    ultilization_square=result["extra_infos"].get("ultilization_square"),
                    yo_construction=result["extra_infos"].get("yo_construction")
                ),
                id=result["id"],
                link=result["link"],
                post_date=result["post_date"],
                created_at=result["created_at"],
                post_id=result["post_id"],
                price=result["price"],
                price_per_square=result["price/square"],
                square=result["square"],
                title=result["title"]
            )
            posts.append(post)
        except (KeyError, ValueError) as e:
            print(f"Error converting result to Post: {e}")
            continue  # Bỏ qua nếu có lỗi trong quá trình chuyển đổi

    return posts
tool = FunctionTool(
    name="get_real_estate_posts",
    description="Get the real estate posts from the database",
    params_json_schema=FunctionArgs.model_json_schema(),
    on_invoke_tool=run_function,
)

database_search_agent = Agent(
    name="The agent searching real estate posts from database",
    model="gpt-4o-mini",
    instructions=f"You are a real estate searching agent. You will be given an input in Vietnamese and you must response in Vietnamese",
    tools=[tool],
    output_type= ListPosts,
    model_settings=ModelSettings(tool_choice="required")
)

async def main():
    result = await Runner.run(database_search_agent, input="lấy cho tôi các căn chung cư mới nhất tại quận Thanh Xuân")
    print(result.final_output)
    print(result.new_items[0].raw_item)
    tool_call_output_item = next(item for item in result.new_items if isinstance(item, ToolCallOutputItem))
    print(type(tool_call_output_item.output))
    print(type(tool_call_output_item.output[0]))
    # posts: ListPosts = result.final_output
    # print(posts)
    # The weather in Tokyo is sunny.
    # for tool in agent.tools:
    #     if isinstance(tool, FunctionTool):
    #         print(tool.name)
    #         print(tool.description)
    #         print(json.dumps(tool.params_json_schema, indent=2))
    #         print(type(tool.params_json_schema))
    #         print()


if __name__ == "__main__":
    asyncio.run(main())