# src/streaming/order_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class OrderAnalytics:
    def __init__(self, spark):
        self.spark = spark
    
    def process_real_time_orders(self):
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            # ... other fields
        ])
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "orders") \
            .load()
        
        orders = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", schema).alias("data")) \
            .select("data.*")
        
        # Calculate metrics
        metrics = orders.groupBy(window("timestamp", "5 minutes")) \
            .agg(
                count("order_id").alias("order_count"),
                avg("order_value").alias("avg_order_value")
            )
        
        return metrics