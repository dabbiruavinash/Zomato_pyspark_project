# src/azure_integration/blob_connector.py
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame

class AzureBlobConnector:
    def __init__(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    def upload_file(self, container_name, blob_name, file_path):
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)