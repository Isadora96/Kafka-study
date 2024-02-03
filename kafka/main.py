from kafka.producer.producer import ProducerAdapter

if __name__ == '__main__':
   kafka = ProducerAdapter() 
   kafka.produce()