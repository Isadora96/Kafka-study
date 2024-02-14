import os
import json
import ast
import requests

from confluent_kafka import Producer

from wikimedia_change_handler import WikimediaChangeHandler

KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER', '')
SECURITY_PROTOCOL = os.environ.get('SECURITY_PROTOCOL', '')
SASL_MECHANISM = os.environ.get('SASL_MECHANISM', '')
USERNAME = os.environ.get('USERNAME', '')
PASSWORD = os.environ.get('PASSWORD', '')
STREAM_URL = os.environ.get('STREAM_URL', '')

class ProducerAdapter:
    
    def __init__(self):
        self.producer = self._connect()
        
    def _connect(self):
        
        conf = {
            'bootstrap.servers': KAFKA_CLUSTER,
            'sasl.mechanisms': SASL_MECHANISM,
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.username': USERNAME,
            'sasl.password': PASSWORD,
            'linger.ms': "20",
            'compression.type': "snappy"
        }

        producer = Producer(conf)
        
        return producer
    
    def get_wikipedia_recent_changes(self):
        url = STREAM_URL
        response = requests.get(url, stream=True)
        
        qnt_message = 0

        for message in response.iter_lines():
            if qnt_message > 5:
                return
            if 'data' in message.decode('utf-8'):
                message = json.loads(message.decode('utf-8').split('data:')[1])
                qnt_message += 1
                self._produce(message)
    
    def _produce(self, message):
        
        topic = 'wikimedia.recentchange'
        
        event_handler = WikimediaChangeHandler(self.producer, topic)
        
        return event_handler.on_message(message)
    
ProducerAdapter().get_wikipedia_recent_changes()