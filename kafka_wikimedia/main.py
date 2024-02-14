import json
import queue

from flask import Flask, render_template, jsonify, Response
from flask_sse import sse
from threading import Thread

from kafka_wikimedia.kafka_producer_wikimedia.wikimedia.wikimedia_changes_producer import ProducerAdapter
from kafka_wikimedia.kafka_consumer_opensearch.opensearch.opensearch_consumer import OpenSearchConsumer

app = Flask(__name__)

task = queue.Queue()

@app.route('/produce_message',  methods=['POST'])
def produce_message():
        
    kafka_message = ProducerAdapter()
    
    kafka_consumer_thread = Thread(target=kafka_message.get_wikipedia_recent_changes)

    if task.empty():
        kafka_consumer_thread.start()
        task.put(kafka_consumer_thread)
        return jsonify({"status": "messages are being produced"})

    for thread in task.queue:
        if not thread.is_alive():
            task.queue.remove(thread)
            task.task_done()
            task.put(kafka_consumer_thread)
            kafka_consumer_thread.start()
            return jsonify({"status": "messages are being produced"})

        return Response(
            response=json.dumps(
                {'Messages being producing already': 'Try another time!'}
            ),
            status=429,
        )


if __name__ == '__main__':
   kafka_consumer = OpenSearchConsumer()
   
   thread_p = Thread(target=app.run)
   thread_c = Thread(target=kafka_consumer.create_index)
   
   thread_p.start()
   thread_c.start()
   
   thread_p.join()
   thread_c.join()