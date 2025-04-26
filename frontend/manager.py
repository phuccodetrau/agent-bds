from __future__ import annotations

import asyncio
import time

from rich.console import Console

from agents import Runner, custom_span, gen_trace_id, trace

from custom_agents.planner import planner_agent
from custom_agents.search_db import Post, ListPosts, database_search_agent
from custom_agents.search_web import search_agent
from custom_agents.bds_writer import ReportData, writer_agent
from printer import Printer
import json

class ResearchManager:
    def __init__(self):
        self.console = Console()
        self.printer = Printer(self.console)

    async def run(self, query: str) -> None:
        trace_id = gen_trace_id()
        with trace("Research trace", trace_id=trace_id):
            self.printer.update_item(
                "trace_id",
                f"View trace: https://platform.openai.com/traces/{trace_id}",
                is_done=True,
                hide_checkmark=True,
            )

            self.printer.update_item(
                "starting",
                "Starting research...",
                is_done=True,
                hide_checkmark=True,
            )
            posts = None
            findings = None
            tool_choices = await self._decide_tool(query)
            print(tool_choices)
            if "search_db" in tool_choices:
                posts = await self._perform_search_db(query)
                with open("tool_call.txt", "a", encoding="utf-8") as f:
                    f.write(str(posts))
            if "search_web" in tool_choices:
                findings = await self._perform_searches(query)

            report = await self._write_report(posts, findings)

            final_report = f"Report summary\n\n{report.short_summary}"
            self.printer.update_item("final_report", final_report, is_done=True)

            self.printer.end()

        print("\n\n=====REPORT=====\n\n")
        print(f"Report: {report.markdown_report}")
        print("\n\n=====FOLLOW UP QUESTIONS=====\n\n")
        follow_up_questions = "\n".join(report.follow_up_questions)
        print(f"Follow up questions: {follow_up_questions}")

    async def _decide_tool(self, query):
        result = await Runner.run(planner_agent, query)
        return json.loads(result.final_output.replace("```", "").replace("json", ""))

    async def _perform_search_db(self, query) -> ListPosts:
        with custom_span("Search the database"):
            result = await Runner.run(database_search_agent, query)
            print(result.to_input_list())
            print(result.final_output_as(ListPosts))
            return result.final_output_as(ListPosts)

    async def _perform_search_web(self, query) -> str:
        with custom_span("Search the web"):
            result = await Runner.run(search_agent, query)
            print(result.to_input_list())
            return str(result.final_output)

    async def _perform_searches(self, query: str) -> list[str]:
        with custom_span("Search the web"):
            self.printer.update_item("searching", "Searching...")
            num_completed = 0
            tasks = [asyncio.create_task(self._search(query))]
            results = []
            for task in asyncio.as_completed(tasks):
                result = await task
                if result is not None:
                    results.append(result)
                num_completed += 1
                self.printer.update_item(
                    "searching", f"Searching... {num_completed}/{len(tasks)} completed"
                )
            self.printer.mark_item_done("searching")
            return results

    async def _search(self, query: str) -> str | None:
        input = f"Search term: {query}"
        try:
            result = await Runner.run(
                search_agent,
                input,
            )
            return str(result.final_output)
        except Exception:
            return None

    async def _write_report(self, query: str, posts: ListPosts = None, findings: list[str] = None) -> ReportData:
        self.printer.update_item("writing", "Thinking about report...")
        input = f"Original query: {query}"
        if posts != None:
            print(input)
            input += f"\nReal estate listings: {posts}"
        if findings != None:
            input += f"\nWeb-sourced data: {findings}"

        result = Runner.run_streamed(
            writer_agent,
            input,
        )
        update_messages = [
            "Thinking about report...",
            "Planning report structure...",
            "Writing outline...",
            "Creating sections...",
            "Cleaning up formatting...",
            "Finalizing report...",
            "Finishing report...",
        ]

        last_update = time.time()
        next_message = 0
        async for _ in result.stream_events():
            if time.time() - last_update > 5 and next_message < len(update_messages):
                self.printer.update_item("writing", update_messages[next_message])
                next_message += 1
                last_update = time.time()

        self.printer.mark_item_done("writing")
        return result.final_output_as(ReportData)
