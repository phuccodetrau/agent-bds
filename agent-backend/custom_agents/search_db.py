from __future__ import annotations
import asyncio
from typing import Optional, List, Any, Literal
from pydantic import BaseModel, Field
from elasticsearch_queries import search_posts, search_posts_strict
from dotenv import load_dotenv
from agents import (
    Agent,
    Runner,
    RunContextWrapper,
    FunctionTool,
    ModelSettings
)
from agents.items import ToolCallOutputItem
from .agent_type import *
load_dotenv()


async def run_function(ctx: RunContextWrapper[Any], args: str) -> List[Post]:
    parsed = FunctionArgs.model_validate_json(args)
    print(parsed)

    search_results = search_posts(
        estate_type=parsed.estate_type,
        is_latest_posted=parsed.is_latest_posted,
        is_latest_created=parsed.is_latest_created,
        district=parsed.district,
        front_face=parsed.front_face,
        front_road=parsed.front_road,
        no_bathrooms=parsed.no_bathrooms,
        no_bedrooms=parsed.no_bedrooms,
        no_floors=parsed.no_floors,
        ultilization_square=parsed.ultilization_square,
        price=parsed.price,
        price_per_square=parsed.price_per_square,
        square=parsed.square,
        description=parsed.description
    )

    posts = []
    for result in search_results:
        try:
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
            continue

    print("no strict")
    print(posts)
    return posts


async def run_function_strict(ctx: RunContextWrapper[Any], args: str) -> List[Post]:
    parsed = FunctionArgs.model_validate_json(args)
    print(parsed)

    search_results = search_posts_strict(
        estate_type=parsed.estate_type,
        is_latest_posted=parsed.is_latest_posted,
        is_latest_created=parsed.is_latest_created,
        district=parsed.district,
        front_face=parsed.front_face,
        front_road=parsed.front_road,
        no_bathrooms=parsed.no_bathrooms,
        no_bedrooms=parsed.no_bedrooms,
        no_floors=parsed.no_floors,
        ultilization_square=parsed.ultilization_square,
        price=parsed.price,
        price_per_square=parsed.price_per_square,
        square=parsed.square,
        description=parsed.description
    )

    posts = []
    for result in search_results:
        try:
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
            continue

    print("strict")
    if len(posts) == 0:
        print("No results from strict search, falling back to non-strict search")
        search_results = search_posts(
            estate_type=parsed.estate_type,
            is_latest_posted=parsed.is_latest_posted,
            is_latest_created=parsed.is_latest_created,
            district=parsed.district,
            front_face=parsed.front_face,
            front_road=parsed.front_road,
            no_bathrooms=parsed.no_bathrooms,
            no_bedrooms=parsed.no_bedrooms,
            no_floors=parsed.no_floors,
            ultilization_square=parsed.ultilization_square,
            price=parsed.price,
            price_per_square=parsed.price_per_square,
            square=parsed.square,
            description=parsed.description
        )

        for result in search_results:
            try:
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
                continue
    print(posts)
    return posts


function_schema = FunctionArgs.model_json_schema()
function_schema["additionalProperties"] = False
function_schema_strict = FunctionArgs.model_json_schema()
function_schema_strict["additionalProperties"] = False
tool = FunctionTool(
    name="get_real_estate_posts",
    description=(
        "Search real estate posts that match **at least one** of the provided criteria. "
        "This tool is suitable when the user is asking about general suggestions or when get_real_estate_posts_strict tool did not return any results."
    ),
    params_json_schema=function_schema,
    on_invoke_tool=run_function,
)
tool_strict = FunctionTool(
    name="get_real_estate_posts_strict",
    description=(
        "Search real estate posts that strictly match **all** of the provided criteria. "
        "Use this tool when the user's question clearly requires that multiple conditions be satisfied at the same time, "
    ),
    params_json_schema=function_schema,
    on_invoke_tool=run_function_strict,
)

database_search_agent = Agent(
    name="The agent searching real estate posts from database",
    model="gpt-4o-mini",
    instructions=f"You are a real estate searching agent. You will be given an input in Vietnamese and you must response in Vietnamese. Return exactly posts from any tool used without independently filtering or editing the information",
    tools=[tool, tool_strict],
    output_type= ListPosts,
    model_settings=ModelSettings(tool_choice="required")
)

async def main():
    result = await Runner.run(database_search_agent, input="Cho tôi các căn hộ 3 phòng ngủ và 2 nhà tắm tại quận Thanh Xuân và Đống Đa")
    print(result.final_output)
    print(result.new_items[0].raw_item)
    tool_call_output_item = next(item for item in result.new_items if isinstance(item, ToolCallOutputItem))
    print(type(tool_call_output_item.output))
    print(type(tool_call_output_item.output[0]))


if __name__ == "__main__":
    asyncio.run(main())