import os

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
            'group.id': 'my-python-app',
            'auto.offset.reset': 'earliest',
        }

        consumer = Consumer(conf)
        
        return consumer
    
    def consume(self):
        
        topic = "demo_python"
    
        self.consumer.subscribe([topic])
        
        no_msg = 0

        try:
            while True:
                
                self._log.info("Polling...")

                msg = self.consumer.poll(2.0)
                if msg is None:
                    no_msg +=1 
                    if no_msg >= 5:
                        return
                    continue
                elif msg.error():
                    self._log.error("ERROR: %s".format(msg.error()))
                else:
                    self._log.info("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8') if msg.key() else '', value=msg.value().decode('utf-8'))
                    )
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self._log.info("Shutting down consumer")
            self.consumer.close()
