"""
Script để đọc dữ liệu từ folder crawl/3 và gửi lên Kafka
"""
import os
import json
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError
import kafka_config

# Đường dẫn đến folder chứa dữ liệu
DATA_FOLDER = Path(__file__).parent.parent / "crawl" / "3"

def create_producer():
    """Tạo Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=kafka_config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        client_id=kafka_config.KAFKA_CLIENT_ID,
        # Cấu hình để đảm bảo gửi thành công
        acks='all',  # Đợi tất cả replicas xác nhận
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    return producer

def read_document(file_path, domain):
    """Đọc nội dung file và trả về dữ liệu dạng dict"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Tạo key từ domain và filename để phân phối đều
        filename = file_path.name
        
        document_data = {
            'domain': domain,
            'filename': filename,
            'content': content,
            'file_path': str(file_path.relative_to(DATA_FOLDER))
        }
        
        return document_data, f"{domain}_{filename}"
    except Exception as e:
        print(f"Lỗi khi đọc file {file_path}: {e}")
        return None, None

def send_documents_to_kafka(producer, data_folder=DATA_FOLDER):
    """Đọc tất cả file trong folder và gửi lên Kafka"""
    total_files = 0
    success_count = 0
    error_count = 0
    
    # Duyệt qua tất cả các domain folder
    for domain_folder in data_folder.iterdir():
        if not domain_folder.is_dir():
            continue
        
        domain = domain_folder.name
        print(f"\nĐang xử lý domain: {domain}")
        
        # Duyệt qua tất cả file .txt trong domain folder
        txt_files = list(domain_folder.glob("*.txt"))
        print(f"  Tìm thấy {len(txt_files)} file")
        
        for file_path in txt_files:
            total_files += 1
            document_data, key = read_document(file_path, domain)
            
            if document_data is None:
                error_count += 1
                continue
            
            try:
                # Gửi message lên Kafka
                future = producer.send(
                    kafka_config.KAFKA_TOPIC,
                    key=key,
                    value=document_data
                )
                
                # Đợi xác nhận (có thể bỏ qua nếu muốn gửi async)
                record_metadata = future.get(timeout=10)
                
                success_count += 1
                if success_count % 10 == 0:
                    print(f"  Đã gửi {success_count} documents...")
                    
            except KafkaError as e:
                error_count += 1
                print(f"  Lỗi khi gửi {file_path.name}: {e}")
            except Exception as e:
                error_count += 1
                print(f"  Lỗi không mong đợi với {file_path.name}: {e}")
    
    # Đảm bảo tất cả messages đã được gửi
    producer.flush()
    
    print(f"\n{'='*50}")
    print(f"Hoàn thành!")
    print(f"Tổng số file: {total_files}")
    print(f"Thành công: {success_count}")
    print(f"Lỗi: {error_count}")
    print(f"{'='*50}")

def main():
    """Hàm main"""
    print("Khởi tạo Kafka Producer...")
    print(f"Bootstrap servers: {kafka_config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {kafka_config.KAFKA_TOPIC}")
    print(f"Data folder: {DATA_FOLDER}")
    
    # Kiểm tra folder tồn tại
    if not DATA_FOLDER.exists():
        print(f"Lỗi: Folder {DATA_FOLDER} không tồn tại!")
        return
    
    try:
        producer = create_producer()
        print("Kết nối Kafka thành công!\n")
        
        send_documents_to_kafka(producer)
        
    except Exception as e:
        print(f"Lỗi khi khởi tạo producer: {e}")
        print("Hãy đảm bảo Kafka đang chạy (docker-compose up)")
    finally:
        if 'producer' in locals():
            producer.close()
            print("\nĐã đóng kết nối Kafka")

if __name__ == "__main__":
    main()

