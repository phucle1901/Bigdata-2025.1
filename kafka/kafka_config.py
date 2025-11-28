"""
Cấu hình cho Kafka
"""

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Topic names
TOPIC_CRAWLED_DOCUMENTS = 'crawled-documents'

# Producer configuration
PRODUCER_CONFIG = {
    'acks': 'all',  # Đợi tất cả replicas acknowledge
    'retries': 3,   # Số lần retry khi gửi message
    'max_in_flight_requests_per_connection': 1,  # Đảm bảo thứ tự message
    'compression_type': 'gzip',  # Nén dữ liệu
}

# Consumer configuration
CONSUMER_CONFIG = {
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'crawl-data-consumer-group',
}
