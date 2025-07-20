import boto3
from datetime import datetime, timedelta

class CostOptimizer:
    def __init__(self, aws_access_key, aws_secret_key):
        self.cloudwatch = boto3.client(
            'cloudwatch',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        self.ec2 = boto3.client(
            'ec2',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
    
    def check_resource_utilization(self, namespace, metric_name, threshold=30):
        end = datetime.utcnow()
        start = end - timedelta(days=1)
        
        response = self.cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            StartTime=start,
            EndTime=end,
            Period=3600,
            Statistics=['Average']
        )
        
        avg_utilization = sum(
            dp['Average'] for dp in response['Datapoints']
        ) / len(response['Datapoints'])
        
        return avg_utilization < threshold
    
    def scale_down_resources(self):
        # Implementation to scale down underutilized resources
        pass