#!/bin/bash

# Crawler parameters
spider_name="bds68_spider"
min_page=1
max_page=25
province="ha-noi"
jump_to_page=1
estate_type=3

# Kafka parameters
kafka_bootstrap_servers="34.72.171.207:9192,34.72.171.207:9292,34.72.171.207:9392"
kafka_topic="bietthu_batch"

# Run crawler
scrapy crawl "$spider_name" \
    -a min_page="$min_page" \
    -a max_page="$max_page" \
    -a province="$province" \
    -a jump_to_page="$jump_to_page" \
    -a estate_type="$estate_type" \
    -s DOWNLOAD_DELAY=5 \
    -s KAFKA_BOOTSTRAP_SERVERS="$kafka_bootstrap_servers" \
    -s KAFKA_TOPIC="$kafka_topic"