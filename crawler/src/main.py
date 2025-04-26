from fastapi import FastAPI, BackgroundTasks
from worker import run_scrapy_crawler
from celery.result import AsyncResult
import redis

app = FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, db=0)


@app.post("/run-crawler/")
def run_crawler(min_page: int, max_page: int, province: str, jump_to_page: int, estate_type: int, batch: int):
    task = run_scrapy_crawler.apply_async(args=[min_page, max_page, province, jump_to_page, estate_type, batch])
    return {"task_id": task.id, "status": "Task started!"}


@app.get("/task-status/{task_id}")
def get_task_status(task_id: str):
    task_result = AsyncResult(task_id, app=run_scrapy_crawler)

    if task_result.state == "PROGRESS":
        return {"task_id": task_id, "status": "RUNNING", "progress": task_result.info}

    if task_result.state == "SUCCESS":
        return {"task_id": task_id, "status": "COMPLETED", "result": task_result.result}

    if task_result.state == "FAILURE":
        return {"task_id": task_id, "status": "FAILED", "error": str(task_result.info)}

    return {"task_id": task_id, "status": task_result.state}
