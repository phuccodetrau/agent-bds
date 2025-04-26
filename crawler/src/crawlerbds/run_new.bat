@echo off
setlocal

:: Crawler parameters
set "spider_name=bds68_spider"
set "min_page=1"
set "max_page=25"
set "province=ha-noi"
set "jump_to_page=1"
set "estate_type=3"

:: Kafka parameters
set "kafka_bootstrap_servers=34.171.201.34:9192,34.171.201.34:9292,34.171.201.34:9392"
set "kafka_topic=bietthu_batch"

:: Run crawler
scrapy crawl %spider_name% ^
    -a min_page=%min_page% ^
    -a max_page=%max_page% ^
    -a province=%province% ^
    -a jump_to_page=%jump_to_page% ^
    -a estate_type=%estate_type% ^
    -s DOWNLOAD_DELAY=5 ^
    -s KAFKA_BOOTSTRAP_SERVERS=%kafka_bootstrap_servers% ^
    -s KAFKA_TOPIC=%kafka_topic%

endlocal
