"""
kafka.bootstrap.servers=46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.mechanism=PLAIN
kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="UnigapKafka@2024";
topic=product_view
"""
"""
Description: Viết chương trình sử dụng Python thực hiện các yêu cầu sau:
- Đọc dữ liệu từ nguồn Kafka được cung cấp trước và produce vào 1 topic trong Kafka bạn đã dựng
- Thực hiện việc đọc dữ liệu từ topic mà bạn tạo ở bước trên và lưu trữ dữ liệu xuống MongoDB
DoD: Source code github và kết quả chương trình đã chạy được
"""

import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import signal
import sys
from typing import Dict, Any, Optional 
import time 
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SOURCE_USERNAME = os.environ['SOURCE_USERNAME'] 
SOURCE_PASSWORD = os.environ['SOURCE_PASSWORD'] 
DESTINATION_USERNAME = os.environ['DESTINATION_USERNAME']
DESTINATION_PASSWORD = os.environ['DESTINATION_PASSWORD']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaBridge:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.consumer = KafkaConsumer(self.config['source_topic'] , **self.config['source'])
        self.producer = KafkaProducer(**self.config['destination'])
        self.running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Optional[Any]) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _transform_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform the message if needed.
        Currently, it just returns the message as is.
        """
        return message
    
    def _on_send_success(self, record_metadata: Any) -> None:
        logger.debug(f"Message sent to topic: {record_metadata.topic}"
                     f" partition: {record_metadata.partition} "
                     f"offset: {record_metadata.offset}")

    def _on_send_error(self, exc: KafkaError) -> None:
        logger.error(f"Failed to send message: {exc}")

    def start(self) -> None:
        logger.info("Starting Kafka Bridge...")
        logger.info(f"Source topic: {self.config['source_topic']}")
        logger.info(f"Destination topic: {self.config['destination_topic']}")
        try:
            message_count = 0
            for message in self.consumer:
                if not self.running:
                    break

                # trasform the message if needed
                transformed_message = self._transform_message(message)
                if transformed_message is None:
                    logger.warning("Received None message, skipping...")
                    continue

                # Produce the message to the destination topic
                future = self.producer.send(
                    self.config['destination_topic'],
                    key=transformed_message.key if transformed_message.key else None,
                    value=transformed_message.value,
                    headers=list(transformed_message.headers.items()) if transformed_message.headers else None
                )

                # Attach callbacks for success and error handling
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)

                message_count += 1

                if message_count % 1000 == 0:
                    logger.info(f"Processed {message_count} messages so far...")
        
        except KafkaError as e:
            logger.error(f"Kafka error occurred: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        logger.info("Cleaning up resources...")
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()

            if self.consumer:
                self.consumer.close()
            logger.info("Resources cleaned up successfully.")
        except KafkaError as e:
            logger.error(f"Error during cleanup: {e}")

def main() :
    # Cau hinh bridge 
    config = {
        'source': {
            'bootstrap_servers': ["46.202.167.130:9094","46.202.167.130:9194","46.202.167.130:9294"],
            'group_id': 'bridge-consumer-group-v1',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'security_protocol': 'SASL_PLAINTEXT',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': SOURCE_USERNAME,
            'sasl_plain_password': SOURCE_PASSWORD,
        },
        'destination': {
            'bootstrap_servers': ["localhost:9094","localhost:9194","localhost:9294"],
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'security_protocol': 'SASL_PLAINTEXT',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': DESTINATION_USERNAME,
            'sasl_plain_password': DESTINATION_PASSWORD,
        },
        'source_topic': 'product_view',
        'destination_topic': 'topic-4-partition-2-rep'
    }

    bridge = KafkaBridge(config)
    bridge.start()


if __name__ == "__main__":
    main()