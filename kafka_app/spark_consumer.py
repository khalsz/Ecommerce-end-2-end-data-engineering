from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, FloatType
import logging
from db_cassandra.populate_cassandra import insert_data_into_cassandra
from minio.minio_connect import put_object

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_spark_session():
    spark = None
    
    try: 
        logger.info("Creating SparkSession")
        spark = SparkSession.builder.appName("SparkConsumer") \
            .master("local[*]") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.driver.memory", "4g") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                                            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
            .getOrCreate()
        logger.info("SparkSession created")
        return spark

        
    except Exception as e:
        logger.error(f"Error creating Spark session {e}")
        

purchase_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("product_id", StringType()), 
                       StructField("time_stamp", StringType()), 
                       StructField("quantity", IntegerType()), 
                       StructField("price",FloatType()), 
                       StructField("payment_type", StringType())])

click_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("product_id", StringType()), 
                       StructField("click_time", StringType())])

search_schema =   StructType([StructField("user_id", StringType()), 
                       StructField("search_time", StringType()), 
                       StructField("search_query", StringType())])


def write_to_dbs(table_name, data): 
    logger.info("Writing to database")
    insert_data_into_cassandra()
    
    put_object()
    logger.info("Written to database")
    
    insert_data_into_cassandra(table_name, data)
    
    put_object(table_name,  data)

def read_stream (spark, schema, topic): 
    try: 
        logger.info("Reading stream")
        
        # Extracting data from kafka stream
        batch_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() \

        
        # Extracting the topic name from the value
        topic = batch_df.selectExpr("CAST(topic AS STRING)").first()[0]
        
        # extracting json_df to insert into databases
        json_df = batch_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
            
            
        logger.info("Stream read successfully")
        return json_df, topic
    except Exception as e:
        logger.error(e)




def write_stream(df, topic): 
    
    try: 
        logger.info("Writing stream")
        query = df.writeStream \
            .foreachBatch(lambda batch_df, _: write_to_dbs(topic, batch_df)) \
            .start()
        query.awaitTermination()
        logger.info("Stream written successfully")
    except Exception as e:
        logger.error(e)
            

if __name__ == "__main__":
    spark = create_spark_session()

    # Reading stream from kafka
    purchase_df, purchase_topic = read_stream(spark, purchase_schema, "purchase")
    click_df, click_topic = read_stream(spark, click_schema, "click")
    search_df, search_topic = read_stream(spark, search_schema, "search")

    # Writing stream to console
    write_stream(purchase_df, purchase_topic)
    write_stream(click_df, click_topic)
    write_stream(search_df, search_topic)
    spark.stop()