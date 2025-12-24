from pyspark.sql.types import *
from pyspark.sql.functions import from_json, when, col, current_timestamp


class Silver:

    def __init__(self, spark):
        wiki_schema = self.Schema()
        parsed_df = self.read_bronze(spark, wiki_schema)
        silver_df = self.silver_transformations(parsed_df)
        self.query = self.write_silver(silver_df)
    
    def Schema(self):
        wiki_schema = StructType([
            StructField("meta", StructType([
                StructField("id", StringType()),
                StructField("dt", TimestampType())
            ])),
            StructField("title", StringType()),
            StructField("namespace", IntegerType()),
            StructField("type", StringType()),
            StructField("user", StructType([
                StructField("id", LongType()),
                StructField("is_bot", BooleanType())
            ])),
            StructField("length", StructType([
                StructField("old", IntegerType()),
                StructField("new", IntegerType())
            ]))
        ])
        return wiki_schema

    def read_bronze(self, spark, wiki_schema):
        bronze_stream_df = spark.readStream.table(
        "wiki.bronze_wiki_events"
        )
        return (
        bronze_stream_df
        .withColumn("parsed", from_json(col("raw_json"), wiki_schema))
        .filter(col("parsed").isNotNull())
        )

    def silver_transformations(self, parsed_df):
        return (
            parsed_df
            .select(
                col("parsed.meta.id").alias("event_id"),
                col("parsed.meta.dt").alias("event_time"),
                col("parsed.title").alias("page_title"),
                col("parsed.namespace"),
                col("parsed.type").alias("change_type"),
                col("parsed.user.id").alias("user_id"),
                col("parsed.user.is_bot").alias("is_bot"),
                (col("parsed.length.new") - col("parsed.length.old")).alias("bytes_diff")
            )
            .withColumn("processing_delay_seconds",current_timestamp().cast("long") - col("event_time").cast("long"))
            .withColumn("edit_size",when(col("bytes_diff") > 500, "LARGE").when(col("bytes_diff") < -500, "LARGE_REMOVAL").otherwise("SMALL"))
            .withColumn("user_type",when(col("is_bot"), "BOT").when(col("user_id").isNull(), "ANONYMOUS").otherwise("REGISTERED"))
            .withWatermark("event_time", "10 minutes")
            .dropDuplicates(["event_id"])
        )

    def write_silver(self, silver_df):
        return (
            silver_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "hdfs://wiki-namenode:9000/user/hive/checkpoints/wiki/silver"
            )
            .toTable("wiki.silver_wiki_events")
        )




