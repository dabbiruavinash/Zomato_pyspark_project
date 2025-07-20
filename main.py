from pyspark.sql import SparkSession
from config_manager import ConfigManager
from kafka.consumers import KafkaStreamProcessor
from batch_processing.customer_segmentation import CustomerSegmentation

class ZomatoAnalyticsApp:
    def __init__(self):
        self.config = ConfigManager()
        self.spark = self._create_spark_session()
        self.kafka_processor = KafkaStreamProcessor(
            self.config.get("kafka.bootstrap_servers"),
            self.config.get("kafka.group_id")
        )
    
    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("ZomatoAnalyticsPlatform") \
            .config("spark.sql.shuffle.partitions", self.config.get("spark.shuffle_partitions", 200)) \
            .config("spark.executor.memory", self.config.get("spark.executor_memory", "8g")) \
            .enableHiveSupport() \
            .getOrCreate()
    
    def run_streaming_jobs(self):
        orders_stream = self.kafka_processor.process_orders()
        # ... other streaming jobs
    
    def run_batch_jobs(self):
        customer_df = self.spark.table("customers")
        segmentation = CustomerSegmentation(self.spark)
        segmented_customers = segmentation.segment_customers(customer_df)
        segmented_customers.write.mode("overwrite").saveAsTable("customer_segments")
        # ... other batch jobs
    
    def run(self):
        self.run_streaming_jobs()
        self.run_batch_jobs()

if __name__ == "__main__":
    app = ZomatoAnalyticsApp()
    app.run()