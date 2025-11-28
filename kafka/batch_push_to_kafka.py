"""
Push dữ liệu vào Kafka theo batch để tối ưu performance
Sử dụng multi-threading để tăng tốc độ
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka_producer import CrawlDataProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchDataPusher:
    """Class để đẩy dữ liệu theo batch với multi-threading"""
    
    def __init__(self, data_path: str = "crawl/3", batch_size: int = 50, num_workers: int = 3):
        """
        Khởi tạo BatchDataPusher
        
        Args:
            data_path: Đường dẫn đến thư mục chứa data
            batch_size: Số lượng documents trong mỗi batch
            num_workers: Số lượng threads
        """
        self.data_path = Path(data_path)
        if not self.data_path.exists():
            raise ValueError(f"Data path không tồn tại: {data_path}")
        
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.producer = CrawlDataProducer()
        
        self.stats = {
            'total_files': 0,
            'success': 0,
            'failed': 0,
            'domains': {}
        }
    
    def extract_metadata(self, content: str, filename: str) -> Dict:
        """Trích xuất metadata từ nội dung"""
        lines = content.strip().split('\n')
        metadata = {
            'filename': filename,
            'content': content,
            'num_lines': len(lines),
            'num_chars': len(content),
        }
        
        if len(lines) > 0:
            metadata['first_line'] = lines[0].strip()
        
        # Tìm số văn bản
        for line in lines[:20]:
            if 'Số:' in line:
                metadata['document_number'] = line.strip()
                break
        
        # Tìm ngày ban hành
        for line in lines[:20]:
            if 'ngày' in line.lower() and 'tháng' in line.lower():
                metadata['issue_date'] = line.strip()
                break
        
        return metadata
    
    def read_file(self, file_path: Path, domain: str) -> Dict:
        """Đọc file và tạo document"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            metadata = self.extract_metadata(content, file_path.name)
            
            return {
                'domain': domain,
                'file_name': file_path.name,
                'file_path': str(file_path),
                **metadata,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Lỗi khi đọc file {file_path}: {e}")
            return None
    
    def process_batch(self, files_batch: List[tuple], domain: str) -> Dict:
        """
        Xử lý một batch files
        
        Args:
            files_batch: List các (file_path, domain)
            domain: Tên domain
            
        Returns:
            Dict kết quả
        """
        documents = []
        
        for file_path in files_batch:
            doc = self.read_file(file_path, domain)
            if doc:
                documents.append(doc)
        
        # Gửi batch vào Kafka
        success = self.producer.send_batch_documents(documents)
        failed = len(documents) - success
        
        return {'success': success, 'failed': failed}
    
    def push_domain_parallel(self, domain_name: str) -> Dict:
        """
        Đẩy documents của domain bằng multi-threading
        
        Args:
            domain_name: Tên domain
            
        Returns:
            Dict thống kê
        """
        domain_path = self.data_path / domain_name
        if not domain_path.exists() or not domain_path.is_dir():
            logger.warning(f"Domain không tồn tại: {domain_name}")
            return {'success': 0, 'failed': 0}
        
        # Lấy tất cả files .txt
        txt_files = list(domain_path.glob('*.txt'))
        total_files = len(txt_files)
        logger.info(f"Bắt đầu đẩy {total_files} files từ domain: {domain_name}")
        
        # Chia thành batches
        batches = [txt_files[i:i + self.batch_size] 
                  for i in range(0, total_files, self.batch_size)]
        
        success_total = 0
        failed_total = 0
        
        # Xử lý parallel với ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # Submit các jobs
            futures = {
                executor.submit(self.process_batch, batch, domain_name): i 
                for i, batch in enumerate(batches)
            }
            
            # Collect results
            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    result = future.result()
                    success_total += result['success']
                    failed_total += result['failed']
                    
                    logger.info(
                        f"  {domain_name} - Batch {batch_idx+1}/{len(batches)}: "
                        f"{result['success']} success, {result['failed']} failed "
                        f"(Tổng: {success_total}/{total_files})"
                    )
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý batch {batch_idx}: {e}")
                    failed_total += len(batches[batch_idx])
        
        logger.info(f"Hoàn thành {domain_name}: {success_total} success, {failed_total} failed")
        return {'success': success_total, 'failed': failed_total}
    
    def push_all_domains(self) -> Dict:
        """Đẩy tất cả domains"""
        domains = [d for d in self.data_path.iterdir() 
                  if d.is_dir() and not d.name.startswith('.')]
        
        logger.info(f"Tìm thấy {len(domains)} domains")
        
        for domain in sorted(domains):
            domain_name = domain.name
            result = self.push_domain_parallel(domain_name)
            
            self.stats['domains'][domain_name] = result
            self.stats['success'] += result['success']
            self.stats['failed'] += result['failed']
            self.stats['total_files'] += result['success'] + result['failed']
        
        return self.stats
    
    def print_stats(self):
        """In thống kê"""
        print("\n" + "="*70)
        print("THỐNG KÊ ĐẨY DỮ LIỆU VÀO KAFKA (BATCH MODE)")
        print("="*70)
        print(f"Batch size: {self.batch_size}")
        print(f"Số workers: {self.num_workers}")
        print(f"Tổng số files: {self.stats['total_files']}")
        print(f"Thành công: {self.stats['success']}")
        print(f"Thất bại: {self.stats['failed']}")
        print(f"Tỷ lệ thành công: {self.stats['success']/max(1, self.stats['total_files'])*100:.2f}%")
        print("\nThống kê theo domain:")
        print("-"*70)
        
        for domain, result in sorted(self.stats['domains'].items()):
            total = result['success'] + result['failed']
            print(f"  {domain:30s}: {result['success']:4d}/{total:4d} files")
        
        print("="*70)
    
    def close(self):
        """Đóng producer"""
        self.producer.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Hàm main"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Đẩy dữ liệu vào Kafka (batch mode)')
    parser.add_argument('--data-path', default='crawl/3',
                       help='Đường dẫn đến thư mục data')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Số lượng documents trong mỗi batch')
    parser.add_argument('--workers', type=int, default=3,
                       help='Số lượng worker threads')
    
    args = parser.parse_args()
    
    with BatchDataPusher(
        data_path=args.data_path,
        batch_size=args.batch_size,
        num_workers=args.workers
    ) as pusher:
        pusher.push_all_domains()
        pusher.print_stats()


if __name__ == "__main__":
    main()
