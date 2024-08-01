import json
from confluent_kafka import Producer, Consumer
import logging
from pathlib import Path
import sys
sys.path.insert(1, str(Path(__file__).parent.parent))
from gen_data.create_data import generate_event_id, Event

    
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Function to handle Kafka message delivery callback
def delivery_report(err, msg):
    """
    Callback function for Kafka message delivery.

    This function is invoked by the Kafka producer after a message is delivered.
    It checks for any errors during delivery and logs the outcome.

    Args:
        err: An error object if the message delivery failed, None otherwise.
        msg: The Kafka message object that was delivered.
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else: 
        print(f'Message delivered to {msg.topic()}') 

# Initializing producer configuration 
config = {
    'bootstrap.servers': 'localhost:9092'
}



def produce_to_kafka(data, topic):
    """
    Produces data to a Kafka topic.

    This function serializes the provided data as JSON and sends it to the specified Kafka topic.

    Args:
        data: The data object to be produced.
        topic: The name of the Kafka topic to produce to.
    """
    try: 
        logger.info("Initializing producer and consumer")
        producer = Producer(config)
        
        logger.info(f"Producing data to topic {topic}")
        
        if data is not None: 
            producer.poll(0)
            serialized_data = json.dumps(data).encode('utf-8')
            producer.produce(topic = topic, value=serialized_data, on_delivery=delivery_report)
            producer.flush()
            logger.info(f"Data produced to topic successfully")
    except Exception as e:
        logger.error(f"Error producing data to topic: {e}")

if __name__ == "__main__":
    for data in range(5): 
        # Generating random user_id, product_id, and event types
        user_id, product_id, event_type = generate_event_id()
        
        # Create class instance of the e-commerce Event
        event = Event(user_id, product_id, event_type)
        
        # Generating data for each event type
        click_data, purchase_data, search_data = event.gen_all_event_data() 
    

        # Producing to Kafka topics
        produce_to_kafka(click_data, "Click")
        produce_to_kafka(purchase_data, "Purchase")
        produce_to_kafka(search_data, "Search")

        