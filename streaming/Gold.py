from pyspark.sql.functions import (col,window,count,sum as spark_sum,avg,approx_count_distinct,when,abs as spark_abs)

class Gold:

    def __init__(self, spark):
        silver_df = self.read_silver(spark)
        gold_df = self.gold_aggregations(silver_df)
        self.query = self.write_gold(gold_df)


    def read_silver(self, spark):
        silver_stream_df = spark.readStream.table(
            "wiki.silver_wiki_events"
        )
        return silver_stream_df
    
    def gold_aggregations(self, silver_df):
        # First, create the windowed aggregation
        windowed_df = (
            silver_df
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), "5 minutes")
            )
            .agg(
                # 1. Platform activity
                count("*").alias("total_edits"),
                # 2. Community engagement
                approx_count_distinct("user_id").alias("unique_editors"),
                # 3. Page activity
                approx_count_distinct("page_title").alias("active_pages"),
                # 4. Automation volume
                spark_sum(
                    when(col("user_type") == "BOT", 1).otherwise(0)
                ).alias("bot_edits"),
                # 5. Human contribution - FIX THE ALIAS HERE
                spark_sum(
                    when(col("user_type") != "BOT", 1).otherwise(0)
                ).alias("human_edits"),  # Changed from "humawiki_lakehouse_maintenance_dag.pyn_edits"
                # 6. Knowledge growth
                spark_sum("bytes_diff").alias("net_bytes_added"),
                # 7. Edit intensity
                avg("bytes_diff").alias("avg_bytes_per_edit"),
                # 8. Large edits (content-heavy changes)
                spark_sum(
                    when(spark_abs(col("bytes_diff")) > 500, 1).otherwise(0)
                ).alias("large_edits_count"),
                # 9. Revert-like behavior (large removals)
                spark_sum(
                    when(col("bytes_diff") < -500, 1).otherwise(0)
                ).alias("revert_like_edits"),
                # 10. Volatility / churn
                spark_sum(
                    spark_abs(col("bytes_diff"))
                ).alias("volatility_score")
            )
        )
        
        # Now flatten the window column and add derived metrics
        return (
            windowed_df
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_edits"),
                col("unique_editors"),
                col("active_pages"),
                col("bot_edits"),
                col("human_edits"),
                col("net_bytes_added"),
                col("avg_bytes_per_edit"),
                col("large_edits_count"),
                col("revert_like_edits"),
                col("volatility_score")
            )
            .withColumn(
                "bot_edit_percentage",
                (col("bot_edits") / col("total_edits")) * 100
            )
            .withColumn(
                "edits_per_minute",
                col("total_edits") / 5.0  # 5 minute window
            )
            .withColumn(
                "knowledge_trend",
                when(col("net_bytes_added") > 0, "GROWING")
                .when(col("net_bytes_added") < 0, "SHRINKING")
                .otherwise("STABLE")
            )
        )
    
    def write_gold(self, gold_df):
        return (
            gold_df.writeStream
            .format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "hdfs://wiki-namenode:9000/user/hive/checkpoints/wiki/gold"
            )
            .option("fanout-enabled", "true")
            .toTable("wiki.gold_wiki_kpis")
        )

