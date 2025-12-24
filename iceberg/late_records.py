from pyspark.sql.functions import from_json, col, window, countDistinct, sum, when, count
from pyspark.sql import SparkSession
from streaming.Silver import Silver

LOOKBACK_HOURS = 24


spark = (
    SparkSession.builder
    .appName("Spark-Iceberg-Hive")
    .master("spark://wiki-spark-master:7077")

    # Hadoop / HDFS
    .config("spark.hadoop.fs.defaultFS", "hdfs://wiki-namenode:9000")

    # Hive Metastore
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://wiki-hive-metastore:9083")

    # Iceberg on spark_catalog (THIS IS THE IMPORTANT PART)
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config(
        "spark.sql.catalog.spark_catalog.warehouse",
        "hdfs://wiki-namenode:9000/user/hive/warehouse"
    )

    # Iceberg extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Performance (optional)
    .config("spark.sql.shuffle.partitions", "200")

    # Enable Hive support
    .enableHiveSupport()
    .getOrCreate()
)

bronze_replay = spark.sql(f"""
    SELECT raw_json
    FROM wiki.bronze_wiki_events
    WHERE ingestion_time >= current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOURS
""")

recomputed_silver = (
    bronze_replay
    .select(from_json(col("raw_json"), Silver.Schema()).alias("d"))
    .select(
        col("d.meta.id").alias("event_id"),
        col("d.meta.dt").cast("timestamp").alias("event_time"),
        col("d.title").alias("page_title"),
        col("d.namespace").alias("namespace"),
        col("d.type").alias("change_type"),
        col("d.user.id").cast("bigint").alias("user_id"),
        col("d.user.is_bot").alias("is_bot"),
        (col("d.length.new") - col("d.length.old")).alias("bytes_diff")
    )
)

recomputed_silver.createOrReplaceTempView("recomputed_silver")

spark.sql("""
    MERGE INTO wiki.silver_wiki_events t
    USING recomputed_silver s
    ON t.event_id = s.event_id

    WHEN MATCHED THEN UPDATE SET
        t.event_time = s.event_time,
        t.page_title = s.page_title,
        t.namespace = s.namespace,
        t.change_type = s.change_type,
        t.user_id = s.user_id,
        t.is_bot = s.is_bot,
        t.bytes_diff = s.bytes_diff

    WHEN NOT MATCHED THEN INSERT *
""")


gold_recompute = (
    spark.table("wiki.silver_wiki_events")
    .where(f"event_time >= current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOURS")
    .groupBy(window(col("event_time"), "10 minutes"))
    .agg(
        count("*").alias("total_edits"),
        countDistinct("user_id").alias("unique_editors"),
        sum(when(col("is_bot"), 1).otherwise(0)).alias("bot_edits"),
        sum(when(~col("is_bot"), 1).otherwise(0)).alias("human_edits"),
        sum("bytes_diff").alias("net_bytes_added")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "total_edits",
        "unique_editors",
        "bot_edits",
        "human_edits",
        "net_bytes_added"
    )
)


