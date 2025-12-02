"""
Script để export thống kê từ Spark logs ra file JSON
"""
import json
import re
from datetime import datetime
import subprocess

def parse_spark_logs():
    """Parse logs từ Spark Consumer"""
    result = subprocess.run(
        ["docker", "logs", "spark_consumer"],
        capture_output=True,
        text=True
    )
    
    logs = result.stdout + result.stderr
    stats = []
    
    # Tìm các batch statistics
    batch_pattern = r'BATCH (\d+) - THỐNG KÊ DỮ LIỆU'
    domain_pattern = r'  • ([^:]+):\s+(\d+) documents'
    total_pattern = r'Tổng số documents: (\d+)'
    size_pattern = r'  • Kích thước (nhỏ nhất|lớn nhất|trung bình|tổng): ([\d,]+) ký tự'
    
    current_batch = None
    current_stats = {}
    
    for line in logs.split('\n'):
        # Tìm batch mới
        batch_match = re.search(batch_pattern, line)
        if batch_match:
            if current_batch is not None:
                stats.append(current_stats)
            current_batch = int(batch_match.group(1))
            current_stats = {
                "batch_id": current_batch,
                "timestamp": datetime.now().isoformat(),
                "total_documents": 0,
                "domains": {},
                "size_stats": {}
            }
        
        # Tìm total documents
        if current_batch is not None:
            total_match = re.search(total_pattern, line)
            if total_match:
                current_stats["total_documents"] = int(total_match.group(1))
            
            # Tìm domain stats
            domain_match = re.search(domain_pattern, line)
            if domain_match:
                domain = domain_match.group(1).strip()
                count = int(domain_match.group(2))
                current_stats["domains"][domain] = count
            
            # Tìm size stats
            size_match = re.search(size_pattern, line)
            if size_match:
                stat_type = size_match.group(1)
                value = int(size_match.group(2).replace(',', ''))
                current_stats["size_stats"][stat_type] = value
    
    if current_batch is not None:
        stats.append(current_stats)
    
    return stats

def export_to_json(stats, filename="spark_stats.json"):
    """Export stats ra file JSON"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"Đã export {len(stats)} batches ra file {filename}")

def print_summary(stats):
    """In tóm tắt thống kê"""
    if not stats:
        print("Chưa có dữ liệu thống kê")
        return
    
    total_docs = sum(s["total_documents"] for s in stats)
    print(f"\n{'='*60}")
    print(f"TỔNG KẾT THỐNG KÊ")
    print(f"{'='*60}")
    print(f"Tổng số batches: {len(stats)}")
    print(f"Tổng số documents đã xử lý: {total_docs:,}")
    
    # Tổng hợp theo domain
    domain_totals = {}
    for stat in stats:
        for domain, count in stat["domains"].items():
            domain_totals[domain] = domain_totals.get(domain, 0) + count
    
    print(f"\nTop 10 Domains:")
    print("-" * 60)
    sorted_domains = sorted(domain_totals.items(), key=lambda x: x[1], reverse=True)
    for domain, count in sorted_domains[:10]:
        print(f"  • {domain:30s}: {count:5d} documents")

if __name__ == "__main__":
    print("Đang parse logs từ Spark Consumer...")
    stats = parse_spark_logs()
    
    if stats:
        export_to_json(stats)
        print_summary(stats)
    else:
        print("Không tìm thấy thống kê trong logs")

