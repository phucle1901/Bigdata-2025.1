"""
Push dữ liệu đã crawl vào Kafka
Đọc các file .txt từ crawl/3/* và gửi vào Kafka
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from kafka_producer import CrawlDataProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPusher:
    """Class để đẩy dữ liệu crawl vào Kafka"""
    
    def __init__(self, data_path: str = "crawl/3"):
        """
        Khởi tạo DataPusher
        
        Args:
            data_path: Đường dẫn đến thư mục chứa data crawl
        """
        self.data_path = Path(data_path)
        if not self.data_path.exists():
            raise ValueError(f"Data path không tồn tại: {data_path}")
        
        self.producer = CrawlDataProducer()
        self.stats = {
            'total_files': 0,
            'success': 0,
            'failed': 0,
            'domains': {}
        }
    
    def extract_metadata_from_content(self, content: str, filename: str) -> Dict:
        """
        Trích xuất metadata từ nội dung văn bản
        
        Args:
            content: Nội dung văn bản
            filename: Tên file
            
        Returns:
            Dict chứa metadata
        """
        lines = content.strip().split('\n')
        metadata = {
            'filename': filename,
            'content': content,
            'num_lines': len(lines),
            'num_chars': len(content),
        }
        
        # Cố gắng trích xuất thông tin cơ bản từ đầu văn bản
        if len(lines) > 0:
            metadata['first_line'] = lines[0].strip()
        
        # Tìm số văn bản (thường ở dòng đầu có "Số:")
        for line in lines[:20]:
            if 'Số:' in line:
                metadata['document_number'] = line.strip()
                break
        
        # Tìm ngày ban hành
        for line in lines[:20]:
            if 'ngày' in line.lower() and 'tháng' in line.lower() and 'năm' in line.lower():
                metadata['issue_date'] = line.strip()
                break
        
        return metadata
    
    def read_file(self, file_path: Path, domain: str) -> Dict:
        """
        Đọc file và tạo document
        
        Args:
            file_path: Đường dẫn file
            domain: Tên domain (Bao-hiem, Bat-dong-san, ...)
            
        Returns:
            Dict chứa thông tin document
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            metadata = self.extract_metadata_from_content(content, file_path.name)
            
            document = {
                'domain': domain,
                'file_name': file_path.name,
                'file_path': str(file_path),
                **metadata,
                'timestamp': datetime.now().isoformat()
            }
            
            return document
            
        except Exception as e:
            logger.error(f"Lỗi khi đọc file {file_path}: {e}")
            raise
    
    def push_domain(self, domain_name: str) -> Dict:
        """
        Đẩy tất cả documents của một domain vào Kafka
        
        Args:
            domain_name: Tên domain
            
        Returns:
            Dict chứa thống kê
        """
        domain_path = self.data_path / domain_name
        if not domain_path.exists() or not domain_path.is_dir():
            logger.warning(f"Domain không tồn tại: {domain_name}")
            return {'success': 0, 'failed': 0}
        
        txt_files = list(domain_path.glob('*.txt'))
        logger.info(f"Bắt đầu đẩy {len(txt_files)} files từ domain: {domain_name}")
        
        success = 0
        failed = 0
        
        for file_path in txt_files:
            try:
                # Đọc file
                document = self.read_file(file_path, domain_name)
                
                # Gửi vào Kafka
                key = f"{domain_name}_{file_path.stem}"
                if self.producer.send_document(document, key=key):
                    success += 1
                    if success % 10 == 0:
                        logger.info(f"  Đã gửi {success}/{len(txt_files)} files từ {domain_name}")
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Lỗi khi xử lý file {file_path}: {e}")
                failed += 1
        
        logger.info(f"Hoàn thành {domain_name}: {success} success, {failed} failed")
        return {'success': success, 'failed': failed}
    
    def push_all_domains(self) -> Dict:
        """
        Đẩy tất cả domains vào Kafka
        
        Returns:
            Dict chứa thống kê tổng thể
        """
        # Lấy danh sách tất cả domains (thư mục con)
        domains = [d for d in self.data_path.iterdir() 
                  if d.is_dir() and not d.name.startswith('.')]
        
        logger.info(f"Tìm thấy {len(domains)} domains")
        logger.info(f"Domains: {[d.name for d in domains]}")
        
        for domain in sorted(domains):
            domain_name = domain.name
            result = self.push_domain(domain_name)
            
            self.stats['domains'][domain_name] = result
            self.stats['success'] += result['success']
            self.stats['failed'] += result['failed']
            self.stats['total_files'] += result['success'] + result['failed']
        
        return self.stats
    
    def push_specific_domains(self, domain_names: List[str]) -> Dict:
        """
        Đẩy các domains cụ thể vào Kafka
        
        Args:
            domain_names: List tên domains
            
        Returns:
            Dict chứa thống kê
        """
        logger.info(f"Đẩy {len(domain_names)} domains được chỉ định")
        
        for domain_name in domain_names:
            result = self.push_domain(domain_name)
            
            self.stats['domains'][domain_name] = result
            self.stats['success'] += result['success']
            self.stats['failed'] += result['failed']
            self.stats['total_files'] += result['success'] + result['failed']
        
        return self.stats
    
    def print_stats(self):
        """In thống kê"""
        print("\n" + "="*60)
        print("THỐNG KÊ ĐẨY DỮ LIỆU VÀO KAFKA")
        print("="*60)
        print(f"Tổng số files: {self.stats['total_files']}")
        print(f"Thành công: {self.stats['success']}")
        print(f"Thất bại: {self.stats['failed']}")
        print(f"Tỷ lệ thành công: {self.stats['success']/max(1, self.stats['total_files'])*100:.2f}%")
        print("\nThống kê theo domain:")
        print("-"*60)
        
        for domain, result in sorted(self.stats['domains'].items()):
            total = result['success'] + result['failed']
            print(f"  {domain:30s}: {result['success']:4d}/{total:4d} files")
        
        print("="*60)
    
    def close(self):
        """Đóng producer"""
        self.producer.close()
    
    def __enter__(self):
        """Context manager enter"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def main():
    """Hàm main"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Đẩy dữ liệu crawl vào Kafka')
    parser.add_argument('--data-path', default='crawl/3', 
                       help='Đường dẫn đến thư mục data')
    parser.add_argument('--domains', nargs='+', 
                       help='Chỉ đẩy các domains cụ thể (cách nhau bởi dấu cách)')
    parser.add_argument('--list-domains', action='store_true',
                       help='Liệt kê tất cả domains và thoát')
    
    args = parser.parse_args()
    
    # Liệt kê domains
    if args.list_domains:
        data_path = Path(args.data_path)
        domains = [d.name for d in data_path.iterdir() 
                  if d.is_dir() and not d.name.startswith('.')]
        print(f"\nTìm thấy {len(domains)} domains trong {args.data_path}:")
        for i, domain in enumerate(sorted(domains), 1):
            num_files = len(list((data_path / domain).glob('*.txt')))
            print(f"{i:2d}. {domain:30s} ({num_files} files)")
        return
    
    # Đẩy data
    with DataPusher(args.data_path) as pusher:
        if args.domains:
            # Đẩy các domains được chỉ định
            pusher.push_specific_domains(args.domains)
        else:
            # Đẩy tất cả domains
            pusher.push_all_domains()
        
        pusher.print_stats()


if __name__ == "__main__":
    main()
