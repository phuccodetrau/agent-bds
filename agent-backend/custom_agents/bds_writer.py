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

class RealEstateAdvice(BaseModel):
    real_estate_findings: str
    """A markdown-formatted summary of listings and web-sourced information."""

    analytics_and_advice: str
    """Detailed analysis and investment recommendations provided by the advisor."""

    follow_up_questions: list[str]
    """Suggested follow-up research topics or questions."""

PROMPT = (
    "You are a professional investment advisor specializing in real estate markets. "
    "You will be given an original investment query along with initial data, such as real estate listings "
    "or web-sourced information compiled by an assistant.\n\n"
    "Your task is to analyze this data, provide a structured summary of the findings, and offer actionable "
    "investment advice based on the given query. Use your expertise to evaluate potential opportunities, "
    "compare listings if applicable, assess risks, and highlight key investment considerations. "
    "If listings are included, cite their links and summarize property specs and noteworthy features in markdown format.\n\n"
    "The output should be well-organized and contain:\n"
    "1. A markdown section summarizing all findings (listings and external info)\n"
    "2. A section with in-depth analysis and personalized advice\n"
    "3. A list of follow-up questions for further research\n\n"
    "Respond entirely in Vietnamese.\n"
    "Only output JSON. Do not include any explanatory text outside the JSON. The output must follow this Pydantic JSON schema:\n"
    f"{RealEstateAdvice.model_json_schema()}"
)


writer_agent = Agent(
    name="WriterAgent",
    instructions=PROMPT,
    model="gpt-4o-mini",
    output_type=RealEstateAdvice
)
