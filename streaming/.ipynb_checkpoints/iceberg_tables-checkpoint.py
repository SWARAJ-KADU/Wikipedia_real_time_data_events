def create_tables(spark):

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS wiki
    """)

    # -------------------------
    # BRONZE
    # -------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS wiki.bronze_wiki_events (
            event_key STRING,
            raw_json STRING,
            topic STRING,
            partition INT,
            offset BIGINT,
            kafka_timestamp TIMESTAMP,
            ingestion_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_time))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'history.expire.max-snapshot-age-ms' = '10800000',  -- 3 hours
            'history.expire.min-snapshots-to-keep' = '12'
        )
    """)

    # -------------------------
    # SILVER
    # -------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS wiki.silver_wiki_events (
            event_id STRING,
            event_time TIMESTAMP,
            page_title STRING,
            namespace INT,
            change_type STRING,
            user_id BIGINT,
            is_bot BOOLEAN,
            bytes_diff INT,
            processing_delay_seconds BIGINT,
            edit_size STRING,
            user_type STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'history.expire.max-snapshot-age-ms' = '7200000',   -- 2 hours
            'history.expire.min-snapshots-to-keep' = '8'
        )
    """)

    # -------------------------
    # GOLD
    # -------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS wiki.gold_wiki_kpis (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            total_edits BIGINT,
            unique_editors BIGINT,
            active_pages BIGINT,
            bot_edits BIGINT,
            human_edits BIGINT,
            net_bytes_added BIGINT,
            avg_bytes_per_edit DOUBLE,
            large_edits_count BIGINT,
            revert_like_edits BIGINT,
            volatility_score BIGINT,
            bot_edit_percentage DOUBLE,
            edits_per_minute DOUBLE,
            knowledge_trend STRING
        )
        USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'history.expire.max-snapshot-age-ms' = '7200000',   -- 2 hours
            'history.expire.min-snapshots-to-keep' = '5'
        )
    """)
