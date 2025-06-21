"""
Description: Viết chương trình sử dụng Python thực hiện các yêu cầu sau:
- Đọc dữ liệu từ nguồn Kafka được cung cấp trước và produce vào 1 topic trong Kafka bạn đã dựng
- Thực hiện việc đọc dữ liệu từ topic mà bạn tạo ở bước trên và lưu trữ dữ liệu xuống MongoDB
DoD: Source code github và kết quả chương trình đã chạy được
"""

"""
In this file, we define the KafkaDestinationConsumer class, 
which is responsible for consuming messages from a Kafka topic and storing them in MongoDB.
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from typing import Dict, Any, Optional
import signal
import sys
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables from .env file

load_dotenv()
DESTINATION_USERNAME = os.environ['DESTINATION_USERNAME']
DESTINATION_PASSWORD = os.environ['DESTINATION_PASSWORD']
MONGODB_USERNAME = os.environ['MONGODB_USERNAME']
MONGODB_PASSWORD = os.environ['MONGODB_PASSWORD']
MONGODB_HOST = os.environ['MONGODB_HOST']
MONGODB_PORT = os.environ['MONGODB_PORT']

# URL encode username/password
username = quote_plus(MONGODB_USERNAME)
password = quote_plus(MONGODB_PASSWORD)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaDestinationConsumer:
    #  Init Kafka Consumer and MongoDB Client
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

        kafka_config = self.config['kafka_destination_consumer']
        kafka_config.update({
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
            'key_deserializer': lambda x: x.decode('utf-8') if x else None
        })

        self.consumer = KafkaConsumer(self.config['topic'], **kafka_config)

        # Connect to mongodb
        self.mongo_client = MongoClient(
            f"mongodb://{username}:{password}@{self.config['mongodb']['host']}:{self.config['mongodb']['port']}/{self.config['mongodb']['database']}",
            authSource='admin', 
            authMechanism='SCRAM-SHA-256'  
        )
        logger.info(f"Test connection to db: {self.mongo_client.admin.command('ping')}")
        self.db = self.mongo_client[self.config['mongodb']['database']]
        self.collection = self.db[self.config['mongodb']['collection']]
        self.running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _process_message(self, message):
        """Process message data and store in mongodb"""
        try:
            document = {
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'timestamp': message.timestamp,
                'key': message.key,
                'value': message.value,
                'header': dict(message.headers or [])
            }

            # Insert into mongodb
            result = self.collection.insert_one(document)
            logger.info(f"Stored message with ID: {result.inserted_id}")

        except Exception as e:
            logger.info(f"Error storing message to MongoDB: {e}")
            raise

    def run(self):
        """Main consumer loop"""
        logger.info(f"Starting consumer for topic: {self.config['topic']}")

        try: 
            for message in self.consumer:
                if not self.running:
                    break
            
                try: 
                    # Process the message
                    self._process_message(message)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self._cleanup()

        

    def _cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        if hasattr(self, 'consumer'):
            self.consumer.close()
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()


if __name__ == "__main__":
    kafka_destination_config = {
        'mongodb' : {
            'host': MONGODB_HOST,
            'port': MONGODB_PORT,
            'database': "kafka-log",
            'collection': "glamira-event"
        },
        'topic': 'topic-4-partition-2-rep',
        'kafka_destination_consumer' : {
            'bootstrap_servers': ["localhost:9094","localhost:9194","localhost:9294"],
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'destination-consumer-group',
            'security_protocol': 'SASL_PLAINTEXT',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': DESTINATION_USERNAME,
            'sasl_plain_password': DESTINATION_PASSWORD,
        }
    }

    try:
        storage_message = KafkaDestinationConsumer(kafka_destination_config)
        storage_message.run()
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Program error: {e}")
        sys.exit(1)
