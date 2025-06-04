from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("UserDataStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.postgresql:postgresql:42.7.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def parse_user_data(df):
    # JSON şemasını tanımla
    user_schema = StructType([
        StructField("results", ArrayType(StructType([
            StructField("gender", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("street", StructType([
                    StructField("number", IntegerType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", StringType(), True),
                StructField("coordinates", StructType([
                    StructField("latitude", StringType(), True),
                    StructField("longitude", StringType(), True)
                ]), True),
                StructField("timezone", StructType([
                    StructField("offset", StringType(), True),
                    StructField("description", StringType(), True)
                ]), True)
            ]), True),
            StructField("email", StringType(), True),
            StructField("login", StructType([
                StructField("uuid", StringType(), True),
                StructField("username", StringType(), True),
                StructField("password", StringType(), True),
                StructField("salt", StringType(), True),
                StructField("md5", StringType(), True),
                StructField("sha1", StringType(), True),
                StructField("sha256", StringType(), True)
            ]), True),
            StructField("dob", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("phone", StringType(), True),
            StructField("cell", StringType(), True),
            StructField("id", StructType([
                StructField("name", StringType(), True),
                StructField("value", StringType(), True)
            ]), True),
            StructField("picture", StructType([
                StructField("large", StringType(), True),
                StructField("medium", StringType(), True),
                StructField("thumbnail", StringType(), True)
            ]), True),
            StructField("nat", StringType(), True)
        ])), True)
    ])
    
    # JSON parse et
    parsed_df = df.select(
        from_json(col("value").cast("string"), user_schema).alias("data")
    )
    
    # Kullanıcıyı al
    user_df = parsed_df.select(
        explode(col("data.results")).alias("user")
    )
    
    # Flattened DataFrame oluştur
    flattened_df = user_df.select(
        concat(substring("user.login.uuid", 1, 4), lit("****-****")).alias("uuid") #col("user.login.uuid").alias("uuid"),
        col("user.gender").alias("gender"),
        col("user.name.title").alias("title"),
        col("user.name.first").alias("first_name"),
        col("user.name.last").alias("last_name"),
        col("user.email").alias("email"),
        expr("concat('***-***-', substring(user.phone, -4))").alias("phone"),  #col("user.phone").alias("phone"),
        expr("concat('***-***-', substring(user.cell, -4))").alias("cell"),  #col("user.cell").alias("cell"),
        to_date(col("user.dob.date")).alias("date_of_birth"),
        col("user.dob.age").alias("age"),
        col("user.nat").alias("nationality"),
        col("user.location.street.number").alias("street_number"),
        col("user.location.street.name").alias("street_name"),
        col("user.location.city").alias("city"),
        col("user.location.state").alias("state"),
        col("user.location.country").alias("country"),
        col("user.location.postcode").cast("string").alias("postcode"),
        col("user.location.coordinates.latitude").cast("decimal(10,8)").alias("latitude"),
        col("user.location.coordinates.longitude").cast("decimal(11,8)").alias("longitude"),
        col("user.location.timezone.offset").alias("timezone_offset"),
        col("user.location.timezone.description").alias("timezone_description"),
        col("user.login.username").alias("username"),
        lit("********").alias("password"), #col("user.login.password").alias("password"),
        col("user.login.salt").alias("salt"),
        col("user.login.md5").alias("md5"),
        col("user.login.sha1").alias("sha1"),
        col("user.login.sha256").alias("sha256"),
        col("user.picture.large").alias("picture_large"),
        col("user.picture.medium").alias("picture_medium"),
        col("user.picture.thumbnail").alias("picture_thumbnail")
    )
    
    return flattened_df

def write_to_postgres(df, epoch_id):
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://pgdb:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "users") \
            .option("user", "postgres") \
            .option("password", "pass") \
            .mode("append") \
            .save()
        df.printSchema()
        df.show()
        logger.info(f"Batch {epoch_id}: {df.count()} kayıt PostgreSQL'e yazıldı")
    except Exception as e:
        logger.error(f"PostgreSQL yazma hatası: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Kafka'dan veri oku
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "user-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Veriyi parse et
    parsed_df = parse_user_data(kafka_df)
    
    # PostgreSQL'e yaz
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    logger.info("Streaming başlatıldı...")
    query.awaitTermination()

if __name__ == "__main__":
    main()