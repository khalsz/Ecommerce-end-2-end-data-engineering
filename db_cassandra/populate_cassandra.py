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



def cassandra_session(): 
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

