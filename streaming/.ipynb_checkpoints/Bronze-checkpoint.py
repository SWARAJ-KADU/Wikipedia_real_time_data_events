from dotenv import load_dotenv
from pyspark.sql.functions import col, current_timestamp
import os
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "wiki-kafka:29092"
TOPIC_NAME = "wiki.raw.events"
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

class Bronze:
    
    def __init__(self, spark):
        raw_kafka_df = self.read_kafka(spark)
        bronze_df = self.bronze_transformations(raw_kafka_df)
        self.query = self.write_bronze(bronze_df)
    
    def read_kafka(self, spark):
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def bronze_transformations(self, raw_kafka_df):
        return( raw_kafka_df.select(
            col("key").cast("string").alias("event_key"),
            col("value").cast("string").alias("raw_json"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_time")
        ))

    def write_bronze(self, bronze_df):
        return (
            bronze_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "hdfs://wiki-namenode:9000/user/hive/checkpoints/wiki/bronze"
            )
            .toTable("wiki.bronze_wiki_events")
        )



