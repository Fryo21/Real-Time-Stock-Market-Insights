"""

Production consumer — Spark Structured Streaming from Kafka to PostgreSQL.



Runs inside Docker via spark-submit (see dockerfile / compose.yml service: consumer).

Reads JSON stock records from the stock_analysis topic and appends them to the stocks table.



For local verification only, use kafka_consumer.py instead.

"""



import os

from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

from pyspark.sql.functions import from_json, col



CHECKPOINT_DIR = "/tmp/checkpoint/kafka_to_postgres"

TOPIC = "stock_analysis"

KAFKA_BOOTSTRAP = "kafka:9092"



POSTGRES_CONFIG = {
    "url": f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB', 'stock_data')}",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "dbtable": "stocks",
    "driver": "org.postgresql.Driver",
}



KAFKA_DATA_SCHEMA = StructType([

    StructField("date", StringType()),

    StructField("symbol", StringType()),

    StructField("open", StringType()),

    StructField("high", StringType()),

    StructField("low", StringType()),

    StructField("close", StringType()),

])



os.makedirs(CHECKPOINT_DIR, exist_ok=True)



spark = (

    SparkSession.builder

    .appName("KafkaSparkStreaming")

    .getOrCreate()

)



df = (

    spark.readStream

    .format("kafka")

    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)

    .option("subscribe", TOPIC)

    .option("startingOffsets", "earliest")

    .option("failOnDataLoss", "false")

    .load()

)



parsed_df = (

    df.selectExpr("CAST(value AS STRING)")

    .select(from_json(col("value"), KAFKA_DATA_SCHEMA).alias("data"))

    .select("data.*")

)



processed_df = parsed_df.select(

    col("date").cast(TimestampType()).alias("date"),

    col("symbol").alias("symbol"),

    col("open").cast(FloatType()).alias("open"),

    col("high").cast(FloatType()).alias("high"),

    col("low").cast(FloatType()).alias("low"),

    col("close").cast(FloatType()).alias("close"),

)





def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():

        return


    (batch_df.write
        .format("jdbc")
        .mode("append")
        .option("url", POSTGRES_CONFIG["url"])
        .option("user", POSTGRES_CONFIG["user"])
        .option("password", POSTGRES_CONFIG["password"])
        .option("dbtable", POSTGRES_CONFIG["dbtable"])
        .option("driver", POSTGRES_CONFIG["driver"])
        .save())





query = (

    processed_df.writeStream

    .foreachBatch(write_to_postgres)

    .option("checkpointLocation", CHECKPOINT_DIR)

    .start()

)



query.awaitTermination()


