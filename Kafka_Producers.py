# src/kafka/producers.py
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
import json

class KafkaEventProducer:
    def __init__(self, bootstrap_servers, schema_registry_url=None):
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'retries': 3,
            'retry.backoff.ms': 1000
        }
        self.json_producer = Producer(self.producer_config)
        
        if schema_registry_url:
            self.producer_config['schema.registry.url'] = schema_registry_url
            self.avro_producer = AvroProducer(self.producer_config)
    
    def produce_order_event(self, order_data):
        self.json_producer.produce(
            topic='order_events',
            value=json.dumps(order_data),
            callback=self._delivery_report
        )
    
    def produce_delivery_event(self, delivery_data):
        self.avro_producer.produce(
            topic='delivery_tracking',
            value=delivery_data,
            value_schema=self._load_avro_schema('delivery_schema.avsc')
        )
    
    def _delivery_report(self, err, msg):
        if err:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')