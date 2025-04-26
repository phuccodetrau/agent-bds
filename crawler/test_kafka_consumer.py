from confluent_kafka import Consumer
import json
from pymongo import MongoClient
import traceback


def connect_mongodb():
    try:
        # Kết nối đến MongoDB Atlas
        client = MongoClient(
            "mongodb+srv://phucnh0703:hoangphuc0703@cluster0.8tjiz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        # Chọn database và collection (ví dụ: database 'test_db' và collection 'messages')
        db = client['test_db_datn']  # Bạn có thể đổi tên database
        collection = db['messages']  # Bạn có thể đổi tên collection
        print("Connected to MongoDB successfully")
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None


def run_consumer():
    server_ip = "34.58.223.132"
    c = Consumer({
        'bootstrap.servers': f'{server_ip}:9192,{server_ip}:9292,{server_ip}:9392',
        'group.id': '1233'
    })
    c.subscribe(['test', "nhapho_batch"])

    # Kết nối MongoDB
    mongo_collection = connect_mongodb()
    if mongo_collection is None:
        print("Cannot proceed without MongoDB connection")
        return

    consumed = []

    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                print(f"Messages consumed: {len(consumed)}")
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            # Lấy message từ Kafka
            msg_received = msg.value()

            try:
                # Giả sử message là JSON, nếu không thì điều chỉnh lại
                msg_data = json.loads(msg_received.decode('utf-8')) if msg_received else {}

                # Thêm thông tin metadata nếu cần
                msg_document = msg_data

                # Đẩy lên MongoDB
                result = mongo_collection.insert_one(msg_document)
                consumed.append(msg_received)
                print(f'Received and stored message: {msg_received}, MongoDB ID: {result.inserted_id}')

            except json.JSONDecodeError:
                # Nếu message không phải JSON, lưu raw data
                msg_document = msg_received.decode('utf-8')
                result = mongo_collection.insert_one(msg_document)
                consumed.append(msg_received)
                print(f'Received and stored raw message: {msg_received}, MongoDB ID: {result.inserted_id}')
            except Exception as e:
                print(f"Error processing message: {str(e)}")
                traceback.print_exc()

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        c.close()
        print("Kafka consumer closed")


if __name__ == '__main__':
    run_consumer()