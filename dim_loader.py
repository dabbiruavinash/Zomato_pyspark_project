from pyspark.sql import DataFrame, functions as F

class DimensionLoader:
    def __init__(self, spark):
        self.spark = spark
    
    def load_restaurant_dimension(self, source_df: DataFrame) -> DataFrame:
        # SCD Type 2 implementation
        current_dim = self.spark.table("dim_restaurant")
        
        # Identify changes
        changes = source_df.join(current_dim, "restaurant_id", "left_outer") \
            .where(
                (source_df["name"] != current_dim["name"]) |
                (source_df["cuisine"] != current_dim["cuisine"]) |
                (current_dim["restaurant_id"].isNull())
            )
        
        # Prepare new version
        new_versions = changes.select(
            source_df["*"],
            F.lit(False).alias("is_current"),
            F.current_timestamp().alias("effective_date"),
            F.lit(None).cast("timestamp").alias("end_date")
        )
        
        # Update existing records
        updated_current = current_dim.join(
            changes.select("restaurant_id"),
            "restaurant_id",
            "left_anti"
        ).withColumn("is_current", F.lit(True))
        
        # Union all records
        return updated_current.unionByName(new_versions)