from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import FloatType
from geopy.distance import geodesic

class RouteOptimizer:
    def __init__(self, spark):
        self.spark = spark
    
    @staticmethod
    def calculate_distance(lat1, lon1, lat2, lon2):
        return geodesic((lat1, lon1), (lat2, lon2)).km
    
    def optimize_delivery_routes(self, orders_df: DataFrame, drivers_df: DataFrame) -> DataFrame:
        # Register UDF
        distance_udf = F.udf(self.calculate_distance, FloatType())
        
        # Cross join orders with available drivers
        candidates = orders_df.crossJoin(drivers_df)
        
        # Calculate distances
        with_distances = candidates.withColumn(
            "distance",
            distance_udf(
                F.col("restaurant_lat"), F.col("restaurant_lon"),
                F.col("driver_lat"), F.col("driver_lon")
            )
        )
        
        # Find closest driver for each order
        optimized = with_distances.groupBy("order_id") \
            .agg(F.min("distance").alias("min_distance")) \
            .join(with_distances, ["order_id", "min_distance"]) \
            .drop("min_distance")
        
        return optimized