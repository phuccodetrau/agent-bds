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

class ReportData(BaseModel):
    short_summary: str
    """A short 2-3 sentence summary of the findings."""

    markdown_report: str
    """The final report"""

    follow_up_questions: list[str]
    """Suggested topics to research further"""
PROMPT = (
    "You are a senior real estate agent tasked with writing a cohesive report for a real estate query. "
    "You will be provided with the original query and initial data, such as real estate listings or "
    "web-sourced information compiled by an assistant.\n"
    "Use your analytical skills to compare and provide investment advice based on the original query "
    "and listings, or offer insights on the web-sourced data. If listings are provided, cite their links "
    "and summarize key details like descriptions and property specs.\n"
    "First, create an outline for the report detailing its structure and flow. Then, generate the report "
    "and return it as your final output.\n"
    "The final output must be in markdown format, lengthy, and detailed. Aim for 5-10 pages of content, "
    "at least 2000 words. You must answer in Vietnamese"
    f"Only output JSON. Do not output anything else. I will be parsing this with Pydantic so output valid JSON only. Follow this JSON schema:{ReportData.model_json_schema()}"
)


writer_agent = Agent(
    name="WriterAgent",
    instructions=PROMPT,
    model="gpt-4o-mini",
    output_type=ReportData
)
