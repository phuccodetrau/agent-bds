import os
import json
import logging
from dotenv import load_dotenv

from confluent_kafka import Consumer

# Setup logging
logging.basicConfig(
    handlers=[
        logging.FileHandler('./local-consumer-log', mode='a'),
        logging.StreamHandler()
    ],
    format='%(asctime)s, %(name)s %(levelname)s %(message)s',
    datefmt='(%Y-%m-%d %H:%M:%S)',
    level=logging.INFO,
    force=True,
)

# Load environment variables
load_dotenv()

DATA_FOLDER = "data"
os.makedirs(DATA_FOLDER, exist_ok=True)  # Đảm bảo thư mục tồn tại

BOOTSTRAP_SERVERS = os.getenv('BOOSTRAP_SERVERS')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID')
SUBSCRIBE_LIST = os.getenv('SUBSCRIBE_LIST', '').split(',')[2:]
POLL_TIMEOUT = float(os.getenv('POLL_TIMEOUT', 0.1))

def get_consumer(bootstrap_servers, group_id, subscribe_list):
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id
    })
    c.subscribe(subscribe_list)
    return c

def append_to_local_file(file_path, msg):
    msg_json = json.dumps(msg, ensure_ascii=False) + '\n'
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(msg_json)
    logging.info(f'Appended message to file: {file_path}')

if __name__ == '__main__':
    consumer = get_consumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID, SUBSCRIBE_LIST)

    count = 0
    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT)

            if msg is None:
                continue
            if msg.error():
                logging.warning(f'Consumer error: {msg.error()}')
                continue

            msg_value = json.loads(msg.value().decode('utf-8'))
            local_file_path = os.path.join(DATA_FOLDER, f'{msg.topic()}.jsonl')
            append_to_local_file(local_file_path, msg_value)

            count += 1
            logging.info(f'Consumed message {count} in topic {msg.topic()}, partition {msg.partition()} at offset {msg.offset()}')
    finally:
        consumer.close()
