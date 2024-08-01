from minio import Minio
from configure.minio_config import config
from io import BytesIO
import json
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def convert_to_bytes(data): 
    """
    Converts data to a byte stream and returns metadata.

    This function takes a dictionary `data` and attempts to convert it to a JSON string encoded in UTF-8.
    It then creates a BytesIO object from the encoded data and returns a tuple containing:

        - The BytesIO object holding the data
        - The length of the encoded data as an integer
        - The filename constructed from the user ID with a ".json" extension

    Args:
        data: A dictionary containing the data to be converted.

    Returns:
        A tuple containing the BytesIO object, data length, and filename, or None on error.
    """
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
    """
        Uploads data to a Minio object storage bucket.

        This function takes a dictionary `data` and a path name. It first converts the data to a byte stream
        using `convert_to_bytes`. Then, it attempts to connect to a Minio client using the access details
        defined in `config['endpoint']`, `config['access_key']`, and `config['secret_key']`. It assumes these
        are defined in a separate configuration file (`configure.minio_config`).

        If connection and data conversion are successful, it uploads the data to the "e-commerce" bucket within
        Minio using the specified path name with the generated filename. The content type is set to "application/json".

        Args:
            data: A dictionary containing the data to be uploaded.
            path_name: The path within the bucket where the data should be stored.

        Returns:
            True on successful upload, False otherwise.
    """
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

    
    
