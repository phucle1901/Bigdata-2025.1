"""
Monitor và verify dữ liệu trong Kafka
Kiểm tra số lượng messages, xem sample data
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CRAWLED_DOCUMENTS
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaMonitor:
    """Monitor Kafka topics và messages"""
    
    def __init__(self):
        """Khởi tạo monitor"""
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
    
    def list_topics(self):
        """Liệt kê tất cả topics"""
        topics = self.admin_client.list_topics()
        print(f"\nTìm thấy {len(topics)} topics:")
        for topic in sorted(topics):
            print(f"  - {topic}")
        return topics
    
    def get_topic_info(self, topic_name: str):
        """Lấy thông tin topic"""
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                print(f"\nTopic '{topic_name}' không có partitions")
                return
            
            print(f"\nThông tin topic: {topic_name}")
            print(f"Số partitions: {len(partitions)}")
            
            total_messages = 0
            for partition in partitions:
                tp = {'topic': topic_name, 'partition': partition}
                consumer.assign([tp])
                consumer.seek_to_beginning(tp)
                start = consumer.position(tp)
                consumer.seek_to_end(tp)
                end = consumer.position(tp)
                messages = end - start
                total_messages += messages
                print(f"  Partition {partition}: {messages} messages")
            
            print(f"Tổng số messages: {total_messages}")
            consumer.close()
            return total_messages
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin topic: {e}")
            return 0
    
    def sample_messages(self, topic_name: str, num_samples: int = 5):
        """Xem sample messages từ topic"""
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            
            print(f"\nSample {num_samples} messages từ topic '{topic_name}':")
            print("="*80)
            
            count = 0
            for message in consumer:
                count += 1
                print(f"\nMessage {count}:")
                print(f"  Key: {message.key.decode('utf-8') if message.key else None}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                
                value = message.value
                print(f"  Domain: {value.get('domain')}")
                print(f"  Filename: {value.get('file_name')}")
                print(f"  Num lines: {value.get('num_lines')}")
                print(f"  Num chars: {value.get('num_chars')}")
                
                if 'document_number' in value:
                    print(f"  Document number: {value['document_number']}")
                if 'issue_date' in value:
                    print(f"  Issue date: {value['issue_date']}")
                
                # In một phần nội dung
                content = value.get('content', '')
                if content:
                    preview = content[:200].replace('\n', ' ')
                    print(f"  Content preview: {preview}...")
                
                if count >= num_samples:
                    break
            
            consumer.close()
            print("="*80)
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy sample messages: {e}")
    
    def analyze_domain_distribution(self, topic_name: str):
        """Phân tích phân phối documents theo domain"""
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=10000  # Timeout sau 10s không có message mới
            )
            
            domain_counter = Counter()
            total = 0
            
            print(f"\nĐang phân tích phân phối domain trong topic '{topic_name}'...")
            
            for message in consumer:
                domain = message.value.get('domain', 'Unknown')
                domain_counter[domain] += 1
                total += 1
                
                if total % 100 == 0:
                    print(f"  Đã xử lý {total} messages...", end='\r')
            
            print(f"\nTổng số messages: {total}")
            print("\nPhân phối theo domain:")
            print("-"*60)
            
            for domain, count in domain_counter.most_common():
                percentage = (count / total * 100) if total > 0 else 0
                print(f"  {domain:30s}: {count:4d} docs ({percentage:5.2f}%)")
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Lỗi khi phân tích domain distribution: {e}")
    
    def verify_data_integrity(self, topic_name: str):
        """Kiểm tra tính toàn vẹn của data"""
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            print(f"\nKiểm tra tính toàn vẹn dữ liệu trong topic '{topic_name}'...")
            
            total = 0
            errors = []
            
            for message in consumer:
                total += 1
                value = message.value
                
                # Kiểm tra các trường bắt buộc
                required_fields = ['domain', 'file_name', 'content']
                for field in required_fields:
                    if field not in value:
                        errors.append(f"Message {message.offset} thiếu field '{field}'")
                
                # Kiểm tra content không rỗng
                if not value.get('content', '').strip():
                    errors.append(f"Message {message.offset} có content rỗng")
                
                if total % 100 == 0:
                    print(f"  Đã kiểm tra {total} messages...", end='\r')
            
            print(f"\nTổng số messages: {total}")
            print(f"Số lỗi tìm thấy: {len(errors)}")
            
            if errors:
                print("\nCác lỗi:")
                for error in errors[:10]:  # Chỉ in 10 lỗi đầu
                    print(f"  - {error}")
                if len(errors) > 10:
                    print(f"  ... và {len(errors) - 10} lỗi khác")
            else:
                print("✓ Dữ liệu hoàn toàn hợp lệ!")
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Lỗi khi verify data: {e}")
    
    def close(self):
        """Đóng admin client"""
        self.admin_client.close()


def main():
    """Hàm main"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor Kafka topics')
    parser.add_argument('--action', choices=['list', 'info', 'sample', 'analyze', 'verify'],
                       default='info', help='Action to perform')
    parser.add_argument('--topic', default=TOPIC_CRAWLED_DOCUMENTS,
                       help='Topic name')
    parser.add_argument('--samples', type=int, default=5,
                       help='Số lượng sample messages')
    
    args = parser.parse_args()
    
    monitor = KafkaMonitor()
    
    try:
        if args.action == 'list':
            monitor.list_topics()
        
        elif args.action == 'info':
            monitor.get_topic_info(args.topic)
        
        elif args.action == 'sample':
            monitor.sample_messages(args.topic, args.samples)
        
        elif args.action == 'analyze':
            monitor.analyze_domain_distribution(args.topic)
        
        elif args.action == 'verify':
            monitor.verify_data_integrity(args.topic)
    
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
