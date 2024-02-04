import threading

from kafka.producer.producer import ProducerAdapter
from kafka.consumer.consumer import ConsumerAdapter

def main():
   kafka_producer = ProducerAdapter()
   kafka_consumer = ConsumerAdapter()
   
   thread_p = threading.Thread(target=kafka_producer.produce)
   thread_c = threading.Thread(target=kafka_consumer.consume)
   
   thread_p.start()
   thread_c.start()
   
   thread_p.join()
   thread_c.join()

if __name__ == '__main__':
   main()