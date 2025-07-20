from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F

class CustomerSegmentation:
    def __init__(self, spark):
        self.spark = spark
    
    def segment_customers(self, customer_df):
        # Feature engineering
        feature_cols = ["order_count", "avg_order_value", "last_order_days"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df = assembler.transform(customer_df)
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df)
        scaled_data = scaler_model.transform(df)
        
        # K-means clustering
        kmeans = KMeans(featuresCol="scaled_features", k=5)
        model = kmeans.fit(scaled_data)
        clustered_data = model.transform(scaled_data)
        
        return clustered_data