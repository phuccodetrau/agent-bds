from __future__ import annotations
import asyncio
from typing import Optional, List, Any
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
import os
load_dotenv()
gemini_api_key = os.getenv("GEMINI_API_KEY")

external_client = AsyncOpenAI(
    api_key=gemini_api_key,
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)

model = OpenAIChatCompletionsModel(
    model="gemini-2.0-flash",
    openai_client=external_client
)

config = RunConfig(
    model=model,
    model_provider=external_client,
    tracing_disabled=True
)

model_settings = ModelSettings(temperature=0, top_p=0.9)


class FunctionArgs(BaseModel):
    estate_type: str
    is_latest_posted: bool = False
    is_latest_created: bool = False
    district: Optional[str] = None
    front_face: Optional[float] = None
    front_road: Optional[float] = None
    no_bathrooms: Optional[int] = None
    no_bedrooms: Optional[int] = None
    no_floors: Optional[int] = None
    ultilization_square: Optional[float] = None
    description: Optional[str] = None

    class Config:
        validate_by_name = True  # Updated from allow_population_by_field_name

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
        validate_by_name = True  # Updated from allow_population_by_field_name
        alias_generator = lambda field_name: field_name.replace("_per_", "/")

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
    instructions=f"You are a real estate searching agent. Only output JSON. Do not output anything else. I will be parsing this with Pydantic so output valid JSON only. Follow this JSON schema:{ListPosts.model_json_schema()}",
    tools=[tool],
    # output_type= ListPosts,
    model_settings=model_settings
)

async def main():
    result = await Runner.run(database_search_agent, input="Give me the latest 'chung cư' posts in Thanh Xuân district?", run_config=config)
    print(result.final_output)
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