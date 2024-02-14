import os

from opensearchpy import OpenSearch

from kafka.logger.logger import get_logger

from kafka_wikimedia.kafka_consumer_opensearch.consumer.consumer import ConsumerAdapter

OPEN_SEARCH_HOST = os.environ.get('OPEN_SEARCH_HOST', '')


class OpenSearchConsumer:
    
    def __init__(self):
        self._logger = get_logger()
        self._client = self._connect()
        self.consumer_kafka = ConsumerAdapter
    
    def _connect(self):
        try:
            # Create the client with SSL/TLS and hostname verification disabled.
            client = OpenSearch(
                hosts = OPEN_SEARCH_HOST,
                http_compress = True, # enables gzip compression for request bodies
                use_ssl = False,
                verify_certs = False,
                ssl_assert_hostname = False,
                ssl_show_warn = False
            )
            
            self._logger.info('Open Search connected!')
            
            return client
        except:
            self._logger.info(f'Error trying to create index {index_name}')
    
    def create_index(self):
        index_name = 'wikimedia'
        
        index_exists = self._client.indices.exists(index_name)
        
        if not index_exists:
            response = self._client.indices.create(index_name)
            print(response)
            self._logger.info(f'Index {index_name} created!')
        else:
            pass
            self._logger.info(f'Index {index_name} already exists!')
            
        self.consumer_kafka().consume(self._client)
