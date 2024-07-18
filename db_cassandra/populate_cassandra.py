from cassandra.cluster import Cluster
import cassandra
from cassandra.auth import PlainTextAuthProvider
import logging
from configure.config_cassandra import db_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Define your credential for db connection
credentials = PlainTextAuthProvider(
    username=db_config["username"], password=db_config["password"]
)


def create_keyspace(session):
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


def create_table(): 
    try: 
        logger.info("Creating Table")
        session = Cluster(["localhost"]).connect()
        session.execute("""
            CREATE TABLE IF NOT EXISTS commerce_stream.purchase (
                user_id UUID PRIMARY KEY,
                product_id UUID,
                time_stamp timestamp,
                quantity int,
                price float,
                total_price float,
                payment_type text 
            ), 
            CREATE TABLE IF NOT EXISTS commerce_stream.click (
                user_id UUID PRIMARY KEY,
                product_id UUID,
                click_time timestamp
            ),
            CREATE TABLE IF NOT EXISTS commerce_stream.search (
                user_id UUID PRIMARY KEY,
                search_time timestamp,
                search_query text
            )
            """)
        logger.info("Table created. ")
        return True
    except Exception as e:
        logger.error("Error creating table. ", e)



def cassandra_session(): 
    try: 
        logger.info("Connecting to Cassandra")
        session = Cluster(["localhost"], auth_provider=credentials).connect()
        
        if session is not None: 
            create_keyspace(session)
            return session
    except cassandra.cluster.NoHostAvailable as e:
        logger.error(f"Error connecting to Cassandra: {e}")
            

def insert_data_into_cassandra(table_name, data):
    create_table() # Create table if it doesn't exist
    _ = cassandra_session()
    try: 
        create_table() # Create table if it doesn't exist
        _ = cassandra_session()
        
        logger.info(f"Inserting data into {table_name}")
        
        data.write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table_name, keyspace="commerce_stream") \
            .save()
    except Exception as e:
        logger.error(f"Error inserting data into Cassandra: {e}")
