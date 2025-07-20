from pyspark.sql import DataFrame

class PostgresConnector:
    def __init__(self, spark, url, user, password):
        self.spark = spark
        self.url = url
        self.user = user
        self.password = password
        self.properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
    
    def read_table(self, table_name) -> DataFrame:
        return self.spark.read \
            .jdbc(url=self.url, table=table_name, properties=self.properties)
    
    def write_table(self, df: DataFrame, table_name, mode="append"):
        df.write \
            .jdbc(url=self.url, table=table_name, mode=mode, properties=self.properties)