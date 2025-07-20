# src/data_quality/validator.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count, lit

class DataValidator:
    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: dict) -> bool:
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        return actual_schema == expected_schema
    
    @staticmethod
    def check_completeness(df: DataFrame) -> DataFrame:
        completeness_report = df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
        )
        return completeness_report
    
    @staticmethod
    def detect_anomalies(df: DataFrame, numeric_cols: list) -> DataFrame:
        stats = df.select(
            [F.mean(col(c)).alias(f"{c}_mean"),
             F.stddev(col(c)).alias(f"{c}_std") 
             for c in numeric_cols]
        ).collect()[0]
        
        anomaly_conditions = None
        for col in numeric_cols:
            mean = stats[f"{col}_mean"]
            std = stats[f"{col}_std"]
            condition = (col(col) < mean - 3*std) | (col(col) > mean + 3*std)
            if anomaly_conditions is None:
                anomaly_conditions = condition
            else:
                anomaly_conditions = anomaly_conditions | condition
        
        return df.withColumn("is_anomaly", when(anomaly_conditions, True).otherwise(False))