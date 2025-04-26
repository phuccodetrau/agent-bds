# from __future__ import annotations
# import asyncio

from agents import Agent, WebSearchTool
from agents.model_settings import ModelSettings
INSTRUCTIONS = (
    "You are a real estate research assistant. Given a search term about real estate, you search the web"
    "for that term and produce a concise summary of results. Focus on real estate info like market trends,"
    "price forecasts, project updates. Summary must be 2-3 paragraphs, under 300 words. Capture main points."
    "Write succinctly, no complete sentences or good grammar needed. For someone synthesizing a real estate"
    "report, so focus on essence, ignore fluff. No extra commentary beyond summary. Just write about the query that you have the information about and ignore the query that you dont't have the information about. You must response in Vietnamese."
)

search_agent = Agent(
    name="Search agent",
    instructions=INSTRUCTIONS,
    tools=[WebSearchTool()],
    model_settings=ModelSettings(tool_choice="required"),
)

# async def main():
#     result = await Runner.run(search_agent, input="Cho tôi các bài đăng nhà phố mới nhất tại quận Hoàn Kiếm có 3 tầng. Cho tôi thông tin về dữ đoán giá trung bình của các căn hộ tại quận Hoàn Kiếm trong 3 tháng tới")
#     print(result.final_output)
#
# if __name__ == "__main__":
#     asyncio.run(main())