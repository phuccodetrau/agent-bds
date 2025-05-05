from __future__ import annotations

import asyncio
import time

from rich.console import Console

from agents import Runner, custom_span, gen_trace_id, trace

from custom_agents.planner import planner_agent
from custom_agents.search_db import Post, ListPosts, database_search_agent
from custom_agents.search_web import search_agent
from custom_agents.bds_writer import RealEstateAdvice, writer_agent
from custom_agents.judge_agent import evaluator
from printer import Printer
import json
from typing import Any

class ResearchManager:
    def __init__(self):
        self.console = Console()
        self.printer = Printer(self.console)

    async def run(self, query: Any) -> None:
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
            if "search_web" in tool_choices:
                findings = await self._perform_searches(query)

            report = await self._write_report(query, posts, findings)

            final_report = f"Findings\n\n{report.real_estate_findings}"
            self.printer.update_item("final_report", final_report, is_done=True)

            self.printer.end()

        print("\n\n=====REPORT=====\n\n")
        print(f"Analytics: {report.analytics_and_advice}")
        print("\n\n=====FOLLOW UP QUESTIONS=====\n\n")
        follow_up_questions = "\n".join(report.follow_up_questions)
        print(f"Follow up questions: {follow_up_questions}")
        return report

    async def _decide_tool(self, query):
        result = await Runner.run(planner_agent, query)
        return json.loads(result.final_output.replace("```", "").replace("json", ""))

    async def _perform_search_db(self, query) -> ListPosts:
        with custom_span("Search the database"):
            input_items = [{"content": query, "role": "user"}]
            posts = []
            for i in range(3):
                result = await Runner.run(database_search_agent, input_items)
                posts = result.final_output_as(ListPosts)
                input_items = result.to_input_list()
                print("Searched posts")
                evaluator_result = await Runner.run(evaluator, input_items)
                eval_result = evaluator_result.final_output
                print(f"Evaluation score: {eval_result.score}")
                if eval_result.score == "pass":
                    print("Done search_db")
                    break

                print("Re-running with feedback")
                input_items.append({"content": f"Feedback: {eval_result.feedback}", "role": "user"})
            return posts


    async def _perform_search_web(self, query) -> str:
        with custom_span("Search the web"):
            result = await Runner.run(search_agent, query)
            return str(result.final_output)

    async def _perform_searches(self, query: Any) -> list[str]:
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

    async def _search(self, query: Any) -> str | None:
        input = f"Search term: {query}"
        try:
            result = await Runner.run(
                search_agent,
                input,
            )
            return str(result.final_output)
        except Exception:
            return None

    async def _write_report(self, query: Any, posts: ListPosts = None, findings: list[str] = None) -> RealEstateAdvice:
        self.printer.update_item("writing", "Thinking about report...")
        input = f"Original query: {query}"
        if posts != None:
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
        return result.final_output_as(RealEstateAdvice)
