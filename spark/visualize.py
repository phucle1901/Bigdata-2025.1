"""
Script để visualize dữ liệu từ Spark Streaming
Có thể chạy độc lập để xem thống kê real-time
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max
import config
import time


def create_spark_session():
    """Tạo Spark Session"""
    spark = SparkSession.builder \
        .appName("DataVisualization") \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def visualize_from_memory_table(spark):
    """Visualize từ in-memory table (nếu có)"""
    try:
        # Thử đọc từ temp view nếu có
        df = spark.sql("SELECT * FROM documents")
        
        print("\n" + "=" * 60)
        print(" THỐNG KÊ TỔNG QUAN")
        print("=" * 60)
        
        # Tổng số documents
        total = df.count()
        print(f"\n Tổng số documents: {total:,}")
        
        # Phân bố theo domain
        print(f"\n Phân bố theo Domain:")
        print("-" * 60)
        domain_df = df.groupBy("domain").agg(
            count("*").alias("count"),
            spark_sum(col("content").length()).alias("total_chars")
        ).orderBy(col("count").desc())
        
        for row in domain_df.collect():
            print(f"  • {row.domain:30s}: {row.count:5d} docs, {row.total_chars:,} chars")
        
        # Thống kê kích thước
        size_df = df.select(col("content").length().alias("size"))
        stats = size_df.agg(
            spark_min("size").alias("min"),
            spark_max("size").alias("max"),
            avg("size").alias("avg"),
            spark_sum("size").alias("total")
        ).collect()[0]
        
        print(f"\n Thống kê kích thước:")
        print("-" * 60)
        print(f"  • Min: {int(stats.min):,} ký tự")
        print(f"  • Max: {int(stats.max):,} ký tự")
        print(f"  • Avg: {int(stats.avg):,} ký tự")
        print(f"  • Total: {int(stats.total):,} ký tự")
        
    except Exception as e:
        print(f"Không có dữ liệu trong memory: {e}")


def main():
    """Hàm main"""
    print("=" * 60)
    print(" SPARK DATA VISUALIZATION")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        while True:
            visualize_from_memory_table(spark)
            print("\n⏳ Đợi 30 giây để cập nhật...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n\n  Đang dừng...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

