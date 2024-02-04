import os

from confluent_kafka import Producer

from kafka.logger.logger import get_logger

KAFKA_CLUSTER = os.environ.get('KAFKA_CLUSTER', '')
SECURITY_PROTOCOL = os.environ.get('SECURITY_PROTOCOL', '')
SASL_MECHANISM = os.environ.get('SASL_MECHANISM', '')
USERNAME = os.environ.get('USERNAME', '')
PASSWORD = os.environ.get('PASSWORD', '')

class ProducerAdapter:
    
    def __init__(self):
        self._log = get_logger()
        self.producer = self._connect()
        
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def callback(self, err, msg):
        if err:
            self._log.error('ERROR: Message failed delivery: {}'.format(err))
        else:
            self._log.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    
    def _connect(self):
        
        conf = {
            'bootstrap.servers': KAFKA_CLUSTER,
            'sasl.mechanisms': SASL_MECHANISM,
            'security.protocol': SECURITY_PROTOCOL,
            'sasl.username': USERNAME,
            'sasl.password': PASSWORD,
        }

        self._log.info("connecting to Kafka topic...")
        producer = Producer(conf)
        
        return producer
    
    def produce(self):
        
        topic = "demo_python"
        msg_value = 'Hello from kafka-study'
        msg_key = '1'
    
        self.producer.produce(topic, msg_value, msg_key, callback=self.callback)
        
        # Block until the messages are sent.
        self.producer.poll(10000)
        self.producer.flush()
