import os
import json

from confluent_kafka import Consumer, OFFSET_BEGINNING

from kafka.logger.logger import get_logger

KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER', '')
SECURITY_PROTOCOL = os.environ.get('SECURITY_PROTOCOL', '')
SASL_MECHANISM = os.environ.get('SASL_MECHANISM', '')
USERNAME = os.environ.get('USERNAME', '')
PASSWORD = os.environ.get('PASSWORD', '')

class ConsumerAdapter:
    
    def __init__(self):
        self._log = get_logger()
        self.consumer = self._connect()
    
    def _connect(self):
        
        conf = {
            'bootstrap.servers': KAFKA_CLUSTER,
            'sasl.mechanisms': SASL_MECHANISM,
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.username': USERNAME,
            'sasl.password': PASSWORD,
            'group.id': 'consumer-opensearch',
            'auto.offset.reset': 'latest'
        }

        consumer = Consumer(conf)
        
        return consumer
    
    def _extract_id(self, value):
        return self._data_to_dict(value)['meta']['id']   
    
    def _data_to_dict(self, value):
        decoded = value.decode('utf-8')
        if 'data' in decoded:
            return json.loads(decoded.split('data:')[1])
    
    def consume(self, opensearch_client):
        
        topic = "wikimedia.recentchange"
        
        index = 'wikimedia'
    
        self.consumer.subscribe([topic])
        
        no_msg = 0

        try:
            while True:
                
                self._log.info("Polling...")

                msg = self.consumer.poll(2.0)
                
                doc_id = self._extract_id(msg.value()) if msg else None                    
                    
                if msg:
                    opensearch_client.index(
                        index = index,
                        body = json.dumps(self._data_to_dict(msg.value())),
                        id = doc_id
                    )
                    self._log.info(f'Inserted document id {doc_id} into OpenSearch _index {index}!')
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self._log.info("Shutting down consumer")
            self.consumer.close()
