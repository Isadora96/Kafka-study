
# from kafka.logger.logger import get_logger

class WikimediaChangeHandler:
    
    def __init__(self, kafka_producer, topic: str):
        self.kafka_producer = kafka_producer
        self.topic = topic
        # # self._log = get_logger()
        
    def callback(self, err, msg):
        if err:
            pass
            # self._log.error('ERROR: Message failed delivery: {}'.format(err))
        else:
            pass
            # self._log.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            # topic=msg.topic(), key=msg.key().decode('utf-8') if msg.key() else '', value=msg.value().decode('utf-8')))
    
    def on_open():
        pass

    def on_pull(self):
        self.kafka_producer.poll(10000)
    
    def on_closed(self):
        self.kafka_producer.flush()
    
    def on_message(self, msg_value):
        self.kafka_producer.produce(self.topic, f'{msg_value}', callback=self.callback)

        self.on_pull()
        self.on_closed()
    
    def on_comment():
        pass
    
    def on_error(self):
        pass
        # self._log.error('Error in stream reading')