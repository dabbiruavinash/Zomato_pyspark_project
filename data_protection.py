from pyspark.sql import DataFrame, functions as F
from cryptography.fernet import Fernet

class DataProtector:
    def __init__(self, encryption_key):
        self.cipher = Fernet(encryption_key)
    
    def encrypt_column(self, df: DataFrame, column: str) -> DataFrame:
        encrypt_udf = F.udf(lambda x: self.cipher.encrypt(x.encode()).decode())
        return df.withColumn(column, encrypt_udf(F.col(column)))
    
    def decrypt_column(self, df: DataFrame, column: str) -> DataFrame:
        decrypt_udf = F.udf(lambda x: self.cipher.decrypt(x.encode()).decode())
        return df.withColumn(column, decrypt_udf(F.col(column)))