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


evaluator = Agent(
    name="real_estate_evaluator",
    instructions=(
        "You will evaluate the list of real estate listings retrieved by the real estate search agent to determine if they meet the user's query. "
        f"The search agent has two tools: simultaneous criteria search named get_real_estate_posts_strict and individual criteria search named get_real_estate_posts (aggregating all listings from each criterion), with identical parameters as follows: {FunctionArgs.model_json_schema()} "
        "If the list of listings is not satisfactory, provide concise feedback to the search agent, indicating whether it is using the appropriate tool and if the user's criteria are being met. Offer brief suggestions based on parameters above to improve the search quality."
        "You must response in Vietnamese"
    ),
    model="gpt-4o-mini",
    output_type=EvaluationFeedback,
)
