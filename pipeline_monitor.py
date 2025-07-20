import logging
from datetime import datetime
from pyspark.sql import SparkSession

class PipelineMonitor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger("PipelineMonitor")
        
    def log_metrics(self, metrics: dict):
        metrics_df = self.spark.createDataFrame([{
            "timestamp": datetime.now(),
            "metric_name": k,
            "metric_value": v
        } for k, v in metrics.items()])
        
        metrics_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("pipeline_metrics")
        
        self.logger.info(f"Logged metrics: {metrics}")
    
    def check_pipeline_health(self):
        # Check Spark UI for running applications
        # Check Kafka consumer lag
        # Check database connections
        pass