import json
from confluent_kafka import Producer, Consumer
from utils import serialize_json, delivery_report
import logging
from gen_data.create_data import generate_event_data, Events

    
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
    

if __name__ == "__main__":
    for data in range(5): 
        # Generating random user_id, product_id, and event types
        event_type, user_id, product_id = generate_event_data()
        
        # Create class instance of the e-commerce Event
        event = Events(user_id, event_type, product_id)
        
        # Generating data for each event type
        purchase_data, click_data, search_data = event.generate_all_data()
        
        # Producing to Kafka topics
        produce_to_kafka('purchase', purchase_data)
        produce_to_kafka('click', click_data)
        produce_to_kafka('search', search_data)
        