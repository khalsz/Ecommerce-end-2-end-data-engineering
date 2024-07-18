import json
from confluent_kafka import Producer, Consumer
from utils import serialize_json, delivery_report
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initializing producer configuration 
config = {
    'bootstrap.servers': 'localhost:9092'
}


# Initializing producer and consumer
producer = Producer(config)
consumer = Consumer(config | {'group.id': 'test-group', 
                              'auto.offset.reset': 'earliest', 
                               'enable.auto.commit': False})


def produce_to_kafka(topic, data):
    """
    Produce data to Kafka topic
    """
    serialized_data = serialize_json(data)
    producer.produce(topic, value=serialized_data, on_delivery=delivery_report)
    producer.poll(0)
    

def consume_from_kafka(*topics):
    """
    Consume data from Kafka topic
    """
    consumed_msg = []
    consumer.subscribe(topics)
    while True: 
        msg = consumer.poll(1)
        if msg is None: 
            continue
        elif msg.error(): 
            logger.error(f"Consumer error: {msg.error()}")
            break
        else: 
            logger.info(f"Consumed message: {msg.value().decode('utf-8')}")
            consumed_msg.append(json.loads(msg.value().decode('utf-8')))
            return consumed_msg
            


