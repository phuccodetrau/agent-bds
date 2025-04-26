from celery import Celery, current_task
import subprocess
import time

REDIS_HOST = "redis"
REDIS_PORT = "6379"

celery_app = Celery(
    "tasks",
    broker=f"redis://{REDIS_HOST}:{REDIS_PORT}/0",
    backend=f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
)


@celery_app.task
def run_scrapy_crawler(min_page, max_page, province, jump_to_page, estate_type, batch):
    spider_name_1 = "bds68_spider"
    spider_name_2 = "bds_spider"
    kafka_bootstrap_servers = "35.224.188.74:9192,35.224.188.74:9292,35.224.188.74:9392"
    kafka_topics = ""
    if estate_type == 0:
        kafka_topics = kafka_topics + "nhapho"
    elif estate_type == 1:
        kafka_topics = kafka_topics + "nharieng"
    elif estate_type == 2:
        kafka_topics = kafka_topics + "chungcu"
    else:
        kafka_topics = kafka_topics + "bietthu"

    if batch == 1:
        kafka_topic = kafka_topics + "_batch"

    current_task.update_state(state="STARTED", meta={"message": "Task started..."})

    command_1 = [
        "scrapy", "crawl", spider_name_1,
        "-a", f"min_page={min_page}",
        "-a", f"max_page={max_page}",
        "-a", f"province={province}",
        "-a", f"jump_to_page={jump_to_page}",
        "-a", f"estate_type={estate_type}"
        "-s", "DOWNLOAD_DELAY=0",
        "-s", f"KAFKA_BOOTSTRAP_SERVERS={kafka_bootstrap_servers}",
        "-s", f"KAFKA_TOPIC={kafka_topic}"
    ]

    command_2 = [
        "scrapy", "crawl", spider_name_2,
        "-a", f"min_page={min_page}",
        "-a", f"max_page={max_page}",
        "-a", f"province={province}",
        "-a", f"jump_to_page={jump_to_page}",
        "-a", f"estate_type={estate_type}"
        "-s", "DOWNLOAD_DELAY=0",
        "-s", f"KAFKA_BOOTSTRAP_SERVERS={kafka_bootstrap_servers}",
        "-s", f"KAFKA_TOPIC={kafka_topic}"
    ]

    try:
        current_task.update_state(state="RUNNING_CMD", meta={"message": "Task running cmd..."})
        subprocess.run(command_1, shell=False, cwd="crawlerbds")
        subprocess.run(command_2, shell=False, cwd="crawlerbds")

        return {"status": "SUCCESS", "message": f"Scrapy crawl finished!"}

    except Exception as e:
        current_task.update_state(state="FAILURE", meta={"message": str(e)})
        raise
