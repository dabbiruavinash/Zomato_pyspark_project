# src/aws_integration/s3_connector.py
import boto3
from pyspark.sql import DataFrame

class S3Connector:
    def __init__(self, spark_session, aws_access_key, aws_secret_key):
        self.spark = spark_session
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
    def write_parquet(self, df: DataFrame, bucket: str, path: str):
        df.write \
            .mode("overwrite") \
            .parquet(f"s3a://{bucket}/{path}")
    
    def read_parquet(self, bucket: str, path: str) -> DataFrame:
        return self.spark.read.parquet(f"s3a://{bucket}/{path}")