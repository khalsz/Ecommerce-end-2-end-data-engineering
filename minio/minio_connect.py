from minio import Minio, MinioException  
from config.minio_config import config
from gen_data.create_data import generate_event_data, Events
from io import BytesIO
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def convert_to_bytes(data): 
    
    # convert to encoded json
    encoded_data = json.dumps(data).encode('utf-8')
    
    # create BytesIO object 
    stream_data = BytesIO()
    stream_data.write(encoded_data)
    
    return stream_data, len(encoded_data)
    
    
    
def put_object(client, path, file_name, stream_record, data_len): 
    try: 
        client.put_object(
        'e-commerce',
        f'{path}/{file_name}',
        data = stream_record,
        length = data_len,
        content_type="application/json"
        )
        logger.info(f"Put object {file_name} to minio")
        return True
    except MinioException as e: 
        logger.error(f"Error oploading object to minio: {e}")

    return False



def send_to_minio(): 
    try: 
        client = Minio(
                config['endpoint'],
                access_key=config['access_key'],
                secret_key=config['secret_key'],
                secure=False
                )
        
        for i in range(20): 
            event_type, user_id, product_id = generate_event_data()
            event = Events(user_id, event_type, product_id)
            purchase_data, click_data, search_data = event.generate_all_data()
            
            if purchase_data and client: 
                stream_purchase, purchase_len = convert_to_bytes(purchase_data)
                file_name = f"{purchase_data["user_id"]}_purchase.json"
                put_object(client, "purchase", file_name, stream_purchase, purchase_len)
                
            if click_data and client:
                stream_click, click_len = convert_to_bytes(click_data)
                file_name = f"{click_data["user_id"]}_click.json"
                put_object(client, "click", file_name, stream_click, click_len)
                
            if search_data and client:
                stream_search, search_len = convert_to_bytes(search_data)
                file_name = f"{search_data["user_id"]}_search.json"
                put_object(client, "search", file_name, stream_search, search_len)
        logger.info("Data sent to minio")
    except MinioException as e:
        logger.error(f"Error connecting to minio: {e}")    
        

    bucket_name = "e-commerce"
    
    
