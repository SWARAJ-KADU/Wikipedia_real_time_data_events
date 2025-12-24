from pyspark.sql import SparkSession
from Bronze import Bronze
from Silver import Silver
from Gold import Gold
from iceberg_tables import create_tables

if __name__ == "__main__":
    
    spark = (
        SparkSession.builder
        .appName("Spark-Iceberg-Hive")
        # .master("spark://wiki-spark-master:7077")  # REMOVE THIS LINE
        # Hadoop / HDFS
        .config("spark.hadoop.fs.defaultFS", "hdfs://wiki-namenode:9000")
        # Hive Metastore
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://wiki-hive-metastore:9083")
        # Iceberg on spark_catalog
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
        # Performance
        .config("spark.sql.shuffle.partitions", "200")
        # Enable Hive support
        .enableHiveSupport()
        .getOrCreate()
    )

    create_tables(spark)
    
    # Start streaming queries
    print("Starting Bronze layer...")
    bronze = Bronze(spark)
    
    print("Starting Silver layer...")
    silver = Silver(spark)
    
    print("Starting Gold layer...")
    gold = Gold(spark)
    
    print("All streaming queries started. Waiting for termination...")
    
    # Keep the application running
    try:
        bronze.query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        bronze.query.stop()
        silver.query.stop()
        gold.query.stop()
        spark.stop()