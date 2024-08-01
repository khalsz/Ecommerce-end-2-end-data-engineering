from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, FloatType
import logging
from db_cassandra.populate_cassandra import insert_data_into_cassandra
from minio_db.minio_connect import put_object

from db_cassandra.populate_cassandra import create_table, cassandra_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Function to create a SparkSession with configurations for Cassandra and Kafka
def create_spark_session():
    """Creates a SparkSession configured for Cassandra and Kafka interactions.

  This function attempts to create a SparkSession with the following configurations:
      - App name: SparkConsumer
      - Master: local[*] (use all available cores)
      - Cassandra connection host: localhost
      - Spark driver memory: 4g
      - Spark jars for Spark SQL Kafka connector and Cassandra connector

  Returns:
      A SparkSession object or None if an error occurs.
  """
    spark = None
    
    try: 
        logger.info("Creating SparkSession")
        spark = SparkSession.builder.appName("SparkConsumer") \
            .master("local[*]") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.driver.memory", "4g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                                            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
            .getOrCreate()
        logger.info("SparkSession created")
        return spark

        
    except Exception as e:
        logger.error(f"Error creating Spark session {e}")
        
# Define schema for different data types (purchase, click, and search)
purchase_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("product_id", StringType()), 
                       StructField("purchase_time", StringType()), 
                       StructField("quantity", IntegerType()), 
                       StructField("price",FloatType()), 
                       StructField("payment_type", StringType())])

click_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("product_id", StringType()), 
                       StructField("click_time", StringType())])

search_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("search_time", StringType()), 
                       StructField("search_query", StringType())])


session = cassandra_session()
create_table() # Create table if it doesn't exist

# Function to write data to Cassandra tables (implementation not shown)
def write_to_dbs(data, tab_name, session, batch_id): 
    logger.info("Writtingn to database")
    try: 
        # Implement logic to write data to Cassandra tables
        insert_data_into_cassandra(data, tab_name, session)
        data.foreach(lambda row: put_object(row.asDict(), tab_name))
    except Exception as e: 
        logger.error(f"Error writing batch {batch_id} to databases: {e}")


# Function to read a stream from Kafka 
def read_stream (spark, schema, topic): 
    """Reads a stream of data from a Kafka topic.

  This function attempts to read a stream of data from a Kafka topic using the provided SparkSession and schema.

  Args:
      spark: A SparkSession object.
      schema: A StructType defining the schema of the data.
      topic: The name of the Kafka topic to read from.

  Returns:
      A DataFrame containing the read data or None if an error occurs.
  """
    try: 
        if spark is not None: 
            logger.info("Reading stream")
            # print("here is the topic", topic)
            # print("here is the spark", spark)
            
            # Extracting data from kafka stream
            batch_df = spark.readStream.format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load().selectExpr("CAST(value AS STRING) as value") 
            
            
            # extracting json_df to insert into databases
            json_df = batch_df\
                .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
                
            print(json_df)    
                
            
            logger.info("Stream read successfully")
            return json_df
    except Exception as e:
        logger.error(e)


# Function to write a stream of data to Cassandra tables
def write_stream(data_df, tab_name, checkpoint, sess): 
    """Writes a stream of DataFrame data to Cassandra tables.

    This function creates a streaming query that writes the data in the DataFrame to a Cassandra table 
    in batches.

    Args:
        data_df: A spark dataframe object.
        tab_name: The name of table used to save data to cassandra and minio.
        checkpoint: WriteStream checkpoint name.
        sess: spark session. 
    Returns:
      A spark query object.
    """
    try: 
        logger.info("Writing stream")
        query = data_df.writeStream \
            .foreachBatch(lambda data, batch_id: write_to_dbs(data, tab_name, sess, batch_id)) \
            .trigger(availableNow=True) \
            .option("checkpointLocation", checkpoint) \
            .start()
        
        logger.info("Stream written successfully")
        return query
        
    except Exception as e:
        
        logger.error(f"Error writing data stream to database {e}")
        
if __name__ == "__main__":
    
    spark = create_spark_session()
    session = cassandra_session()
    
    # read_stream(spark, purchase_schema, "purchase")
    # # Reading stream from kafka
    purchase_df = read_stream(spark=spark, schema=purchase_schema, topic="Purchase")
    click_df = read_stream(spark=spark, schema=click_schema, topic="Click")
    search_df = read_stream(spark=spark, schema=search_schema, topic="Search")

    
    query1 = write_stream(purchase_df, "purchase", "purchase_check1", session)
    query2 = write_stream(click_df, "click", "purchase_check2", session)
    query3 = write_stream(search_df, "search", "purchase_check3", session)
    
    
    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    
    
    spark.stop()