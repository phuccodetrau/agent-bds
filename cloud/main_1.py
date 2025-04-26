import os
import json
import logging
from dotenv import load_dotenv

from google.cloud import storage
from confluent_kafka import Consumer

logging.basicConfig(
    handlers=[
        logging.FileHandler('./gcs-consumer-log', mode='a'),
        logging.StreamHandler()
    ],
    format='%(asctime)s, %(name)s %(levelname)s %(message)s',
    datefmt='(%Y-%m-%d %H:%M:%S)',
    level=logging.INFO,
    force=True,
)

load_dotenv()

# Load environment variables
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH')
BOOSTRAP_SERVERS = os.getenv('BOOSTRAP_SERVERS')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID')
SUBSCRIBE_LIST = os.getenv('SUBSCRIBE_LIST', '').split(',')[2:]
POLL_TIMEOUT = float(os.getenv('POLL_TIMEOUT', 0.1))

# Initialize GCS client
storage_client = storage.Client.from_service_account_json(GCS_CREDENTIALS_PATH)
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def get_consumer(bootsrap_servers, group_id, subscribe_list):
    c = Consumer({
        'bootstrap.servers': bootsrap_servers,
        'group.id': group_id
    })
    c.subscribe(subscribe_list)
    return c


def push_to_gcs(gcs_file_path, msg):
    blob = bucket.blob(gcs_file_path)

    # Chuyển JSON thành chuỗi UTF-8
    msg_json = json.dumps(msg, ensure_ascii=False) + '\n'

    if blob.exists():
        existing_data = blob.download_as_text(encoding='utf-8')
        updated_data = existing_data + msg_json
    else:
        updated_data = msg_json

    # Upload với encoding đúng
    blob.upload_from_string(updated_data.encode('utf-8'), content_type='text/plain;charset=utf-8')
    logging.info(f'Uploaded message to GCS: {gcs_file_path}')


if __name__ == '__main__':
    consumer = get_consumer(BOOSTRAP_SERVERS, CONSUMER_GROUP_ID, SUBSCRIBE_LIST)

    count = 0
    while True:
        msg = consumer.poll(POLL_TIMEOUT)

        if msg is None:
            continue
        if msg.error():
            logging.info(f'Consumer error: {msg.error()}.')
            continue

        msg_value = json.loads(msg.value().decode('utf-8'))
        gcs_file_path = f'{msg.topic()}/{msg.topic()}.jsonl'
        push_to_gcs(gcs_file_path, msg_value)
        count += 1
        logging.info(f'Consumed message {count} in topic {msg.topic()}, partition {msg.partition()} at offset {msg.offset()}')

    consumer.close()