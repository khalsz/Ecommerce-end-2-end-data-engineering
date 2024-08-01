from minio import Minio
from configure.minio_config import config
from io import BytesIO
import json
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def convert_to_bytes(data): 
    try: 
        file_name = f"{data['user_id']}.json"
        # convert to encoded json
        encoded_data = json.dumps(data).encode('utf-8')
        # create BytesIO object 
        stream_data = BytesIO(encoded_data)
        return stream_data, len(encoded_data), file_name
        
    except Exception as e: 
        logger.error(f"Error converting data to bytes: {e}")
        return None, 0, None
    
    
def put_object(data, path_name): 
    
    try: 
        logger.info(f"Putting object data to minio")
        stream_record, data_len, file_name = convert_to_bytes(data)
        stream_record.seek(0)
        

        if (data is not None ) and stream_record: 
            client = Minio(
                            config['endpoint'], 
                            access_key=config['access_key'],
                            secret_key=config['secret_key'], 
                            secure=False
                        )
            bucket_name = "e-commerce"
            client.put_object(
                    bucket_name,
                    f'{path_name}/{file_name}',
                    data = stream_record,
                    length = data_len,
                    content_type="application/json"
            )
            logger.info(f"Put object {file_name} to minio")
            return True
    except Exception as e: 
        logger.error(f"Error oploading object to minio: {e}")

        return False

    
    
