from pyspark.sql import SparkSession

def expire_snapshots(spark):

    spark.sql("""
        CALL system.expire_snapshots(
            table => 'wiki.gold_wiki_kpis',
            older_than => current_timestamp() - INTERVAL 2 HOURS,
            retain_last => 5
        )
    """)

    spark.sql("""
        CALL system.expire_snapshots(
            table => 'wiki.silver_wiki_events',
            older_than => current_timestamp() - INTERVAL 2 HOURS,
            retain_last => 8
        )
    """)

    spark.sql("""
        CALL system.expire_snapshots(
            table => 'wiki.bronze_wiki_events',
            older_than => current_timestamp() - INTERVAL 3 HOURS,
            retain_last => 12
        )
    """)


def remove_orphan_files(spark):

    spark.sql("""
        CALL system.remove_orphan_files(
            table => 'wiki.gold_wiki_kpis',
            older_than => current_timestamp() - INTERVAL 2 HOURS
        )
    """)

    spark.sql("""
        CALL system.remove_orphan_files(
            table => 'wiki.silver_wiki_events',
            older_than => current_timestamp() - INTERVAL 2 HOURS
        )
    """)

    spark.sql("""
        CALL system.remove_orphan_files(
            table => 'wiki.bronze_wiki_events',
            older_than => current_timestamp() - INTERVAL 3 HOURS
        )
    """)

def compact_data_files(spark):

    spark.sql("""
        CALL system.rewrite_data_files(
            table => 'wiki.gold_wiki_kpis',
            options => map(
                'target-file-size-bytes','536870912',
                'min-input-files','5'
            )
        )
    """)

    spark.sql("""
        CALL system.rewrite_data_files(
            table => 'wiki.silver_wiki_events',
            options => map(
                'target-file-size-bytes','268435456'
            )
        )
    """)

    spark.sql("""
        CALL system.rewrite_data_files(
            table => 'wiki.bronze_wiki_events',
            options => map(
                'target-file-size-bytes','134217728'
            )
        )
    """)

def rewrite_manifests(spark):

    spark.sql("""
        CALL system.rewrite_manifests(
            'wiki.gold_wiki_kpis'
        )
    """)

    spark.sql("""
        CALL system.rewrite_manifests(
            'wiki.silver_wiki_events'
        )
    """)
    spark.sql("""
        CALL system.rewrite_manifests(
            'wiki.bronze_wiki_events'
        )
    """)

if __name__ == "__main__":

    
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

    # Run optimization tasks
    expire_snapshots(spark)
    remove_orphan_files(spark)
    compact_data_files(spark)
    rewrite_manifests(spark)