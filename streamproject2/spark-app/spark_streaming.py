import logging
import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

import os
os.environ['HADOOP_USER_NAME'] = 'sparkuser'

# Graceful shutdown için signal handler
def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    if 'stream_query' in globals():
        stream_query.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")

def initialize_spark_session(app_name):
    try:
        # spark-submit ile çalıştırıldığında master URL otomatik olarak set edilir
        # Bu yüzden .master() çağrısı yapmayın
        spark = (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.streaming.checkpointLocation", "/tmp/postgres-checkpoint")
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "600s")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Arrow optimizasyonunu kapat
            .getOrCreate())
         
        spark.sparkContext.setLogLevel("WARN")
        
        # Master URL'yi logla
        logger.info(f'Spark session initialized successfully')
        logger.info(f'Master URL: {spark.sparkContext.master}')
        logger.info(f'App Name: {spark.sparkContext.appName}')
        logger.info(f'Spark Version: {spark.version}')
        
        return spark
     
    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None

def get_streaming_dataframe(spark, brokers, topic):
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", "1000") \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        return df
     
    except Exception as e:
        logger.error(f"Failed to fetch streaming dataframe. Error: {e}")
        return None

def transform_streaming_data(df):
    # Producer'dan gelen veri formatına uygun schema
    schema = StructType([
        StructField("name", StringType(), True),           # Producer: name
        StructField("gender", StringType(), True),         # Producer: gender  
        StructField("address", StringType(), True),        # Producer: address
        StructField("city", StringType(), True),           # Producer: city
        StructField("nation", StringType(), True),         # Producer: nation
        StructField("zip", LongType(), True),              # Producer: zip (encrypted)
        StructField("latitude", FloatType(), True),        # Producer: latitude
        StructField("longitude", FloatType(), True),       # Producer: longitude
        StructField("email", StringType(), True),          # Producer: email
        StructField("timestamp", LongType(), True)         # Producer: timestamp
    ])
    
    transformed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), schema).alias("data")) \
        .select("data.*") \
        .filter("name IS NOT NULL") \
        .select(
            col("name").alias("full_name"),
            col("gender"),
            col("address").alias("location"), 
            col("city"),
            col("nation").alias("country"),
            col("zip").alias("postcode"),
            col("latitude"),
            col("longitude"),
            col("email"),
            col("timestamp")
        )
    
    return transformed_df

def initiate_streaming_to_postgre(df, jdbc_url, table_name, user, password, checkpoint_location):
    logger.info("Initiating streaming process to PostgreSQL...")
    
    def write_to_postgres(batch_df, epoch_id):
        try:
            batch_count = batch_df.count()
            logger.info(f"Processing batch {epoch_id} with {batch_count} records")
            
            if batch_count > 0:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .option("batchsize", "1000") \
                    .mode("append") \
                    .save()
                    
                logger.info(f"Batch {epoch_id} written successfully")
            else:
                logger.info(f"Batch {epoch_id} is empty, skipping...")
            
        except Exception as e:
            logger.error(f"Error writing batch {epoch_id}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise e
    
    global stream_query
    stream_query = df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    logger.info("Streaming query started, waiting for termination...")
    stream_query.awaitTermination()

def main():
    app_name = "SparkStructuredStreamingToPostgre"
    brokers = "kafka_broker:19092"
    topic = "names_topic"
    # Docker Compose'daki airflow_db servisini kullan
    jdbc_url = "jdbc:postgresql://airflow_db:5432/postgres"
    table_name = "personel_tbl"
    user = "postgres"
    password = "postgres"
    checkpoint_location = "/tmp/postgres-checkpoint"
    
    logger.info(f"Starting {app_name}")
    
    spark = initialize_spark_session(app_name)
    if spark:
        try:
            df = get_streaming_dataframe(spark, brokers, topic)
            if df:
                transformed_df = transform_streaming_data(df)
                initiate_streaming_to_postgre(
                    transformed_df,
                    jdbc_url,
                    table_name,
                    user,
                    password,
                    checkpoint_location
                )
        except Exception as e:
            logger.error(f"Main execution error: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
        finally:
            logger.info("Stopping Spark session...")
            spark.stop()
    else:
        logger.error("Failed to initialize Spark session")
        sys.exit(1)

if __name__ == '__main__':
    main()