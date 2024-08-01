from cassandra.cluster import Cluster
import cassandra
from cassandra.auth import PlainTextAuthProvider
import logging

from pathlib import Path

import sys
sys.path.insert(1, str(Path(__file__).parent.parent))

print(sys.path)

from configure.config_cassandra import db_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Define your credential for db connection
credentials = PlainTextAuthProvider(
    username=db_config["username"], password=db_config["password"]
)


# Function to create a Cassandra keyspace named "commerce_stream"
def create_keyspace(session):
    """
    Creates a Cassandra keyspace named "commerce_stream" if it doesn't exist.

    This function attempts to create a keyspace named "commerce_stream" with a replication strategy
    using 'SimpleStrategy' and a replication factor of 1 (adjust as needed).

    Args:
        session: A Cassandra session object.

    Raises:
        Exception: If an error occurs while creating the keyspace.
    """
    try: 
        logger.info("Creating Keyspace")
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS commerce_stream 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}            
            """)
        session.set_keyspace("commerce_stream")
        logger.info("Keyspace created. ")
    except Exception as e: 
        logger.error(f"Error connecting to Cassandra: {e}")


# Function to create Cassandra tables for purchases, clicks, and searches
def create_table():
    """
    Creates Cassandra tables named "purchase", "click", and "search" if they don't exist.

    This function connects to a Cassandra cluster on localhost and attempts to create three tables:
        - purchase: Stores purchase data with user_id, product_id, purchase_time, quantity, price, and payment_type.
        - click: Stores click data with user_id, product_id, and click_time.
        - search: Stores search data with user_id, search_time, and search_query.

    The tables have appropriate primary keys defined.

    Returns:
        True on successful table creation, raises an exception otherwise.
    """
    try: 
        logger.info("Creating Table")
        session = Cluster(["localhost"]).connect()
        session.execute("""
            CREATE TABLE IF NOT EXISTS commerce_stream.purchase (
                user_id text,
                product_id text,
                purchase_time text,
                quantity int,
                price float, 
                payment_type text, 
                PRIMARY KEY (user_id, purchase_time)
            )
            """)
        
 
        session.execute("""
            CREATE TABLE IF NOT EXISTS commerce_stream.click (
                user_id UUID,
                product_id UUID,
                click_time text, 
                PRIMARY KEY (user_id, click_time)
            )
            """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS commerce_stream.search (
                user_id UUID,
                search_time text,
                search_query text, 
                PRIMARY KEY (user_id, search_time)
            )
            """)
        
 
        logger.info("Table created. ")
        return True
    except Exception as e:
        logger.error("Error creating table. ", e)



# Function to establish a connection to the Cassandra cluster
def cassandra_session():
    """
    Establishes a connection to the Cassandra cluster on localhost.

    This function attempts to connect to the Cassandra cluster at "localhost" using the provided credentials
    (assumed to be defined elsewhere). If successful, it creates the "commerce_stream" keyspace if it doesn't exist
    and returns the session object.

    Returns:
        A Cassandra session object on successful connection, None otherwise.
    """ 
    try: 
        logger.info("Connecting to Cassandra")
        print(credentials)
        session = Cluster(["localhost"], auth_provider=credentials).connect()
        
        if session is not None: 
            create_keyspace(session)
            return session
    except cassandra.cluster.NoHostAvailable as e:
        logger.error(f"Error connecting to Cassandra: {e}")
            

def insert_data_into_cassandra(data, tab_name, session):
    """
    Inserts data from a Spark DataFrame into a Cassandra table.

    This function checks if the data DataFrame and session object are valid. If so, it attempts to write the data
    in append mode to the specified table name within the "commerce_stream" keyspace using Spark SQL's Cassandra connector.

    Args:
        data: A Spark DataFrame containing the data to be inserted.
        tab_name: The name of the Cassandra table to insert data into.
        session: A Cassandra session object.
    """
    try: 
        if (data is not None) and session: 
            data.show()
            logger.info(f"Inserting data into {tab_name} table")
            
            data.write.format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=tab_name, keyspace="commerce_stream") \
                .save() 
        else: 
            print("error writing")
    except Exception as e:
        logger.error(f"Error inserting data into Cassandra: {e}")



if __name__ == "__main__": 
    cassandra_session()
    create_table()

