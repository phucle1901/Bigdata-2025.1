"""
Script đơn giản để đẩy data vào Kafka
Sử dụng khi muốn test nhanh hoặc đẩy ít data
"""

from push_data_to_kafka import DataPusher

# Cách 1: Đẩy tất cả data
print("Bắt đầu đẩy tất cả data vào Kafka...")
with DataPusher(data_path="crawl/3") as pusher:
    pusher.push_all_domains()
    pusher.print_stats()

# Cách 2: Chỉ đẩy một số domains cụ thể
# with DataPusher(data_path="crawl/3") as pusher:
#     pusher.push_specific_domains(['Bao-hiem', 'Bat-dong-san'])
#     pusher.print_stats()

# Cách 3: Đẩy từng domain riêng lẻ
# with DataPusher(data_path="crawl/3") as pusher:
#     result = pusher.push_domain('Bao-hiem')
#     print(f"Kết quả: {result}")
