"""
Kafka Producer để gửi dữ liệu crawl vào Kafka
"""

import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    TOPIC_CRAWLED_DOCUMENTS,
    PRODUCER_CONFIG
)

logger = logging.getLogger(__name__)


class CrawlDataProducer:
    """Producer để gửi dữ liệu crawl vào Kafka"""
    
    def __init__(self, 
                 bootstrap_servers: list = None,
                 topic: str = None):
        """
        Khởi tạo Kafka Producer
        
        Args:
            bootstrap_servers: Danh sách Kafka bootstrap servers
            topic: Tên topic để gửi dữ liệu
        """
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or TOPIC_CRAWLED_DOCUMENTS
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                **PRODUCER_CONFIG
            )
            logger.info(f"Kafka Producer được khởi tạo: {self.bootstrap_servers}")
            logger.info(f"Topic: {self.topic}")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka Producer: {e}")
            raise
    
    def send_document(self, document: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Gửi một document vào Kafka
        
        Args:
            document: Dict chứa thông tin document
            key: Key cho message (optional)
            
        Returns:
            True nếu gửi thành công, False nếu thất bại
        """
        try:
            future = self.producer.send(
                self.topic,
                value=document,
                key=key
            )
            
            # Đợi xác nhận từ Kafka (blocking)
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error khi gửi message: {e}")
            return False
        except Exception as e:
            logger.error(f"Lỗi khi gửi document: {e}")
            return False
    
    def flush(self):
        """Flush tất cả messages chưa được gửi"""
        try:
            self.producer.flush()
            logger.info("Đã flush tất cả messages")
        except Exception as e:
            logger.error(f"Lỗi khi flush: {e}")
    
    def close(self):
        """Đóng producer"""
        try:
            self.producer.close()
            logger.info("Đã đóng Kafka Producer")
        except Exception as e:
            logger.error(f"Lỗi khi đóng producer: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.flush()
        self.close()
