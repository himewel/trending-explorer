import logging

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, lit
from pyspark.sql.types import (
    ArrayType,
    LongType,
    TimestampType,
    StringType,
    StructField,
    StructType,
)


class TweetIngestion:
    def __init__(self, landing_path, raw_path):
        self.landing_path = landing_path
        self.raw_path = raw_path

    def get_spark(self):
        builder = (
            SparkSession.builder.appName("tweet_ingestion")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    def get_field_list(self):
        schema = StructType(
            [
                StructField("author_id", StringType()),
                StructField("created_at", TimestampType()),
                StructField("id", StringType()),
                StructField("lang", StringType()),
                StructField(
                    "public_metrics",
                    StructType(
                        [
                            StructField("like_count", LongType()),
                            StructField("quote_count", LongType()),
                            StructField("reply_count", LongType()),
                            StructField("retweet_count", LongType()),
                        ]
                    ),
                ),
                StructField("text", StringType()),
                StructField(
                    "watermark",
                    StructType(
                        [
                            StructField("end_time", TimestampType()),
                            StructField("expansions", ArrayType(StringType())),
                            StructField("max_results", LongType()),
                            StructField("query", StringType()),
                            StructField("start_time", TimestampType()),
                            StructField("tweet_fields", ArrayType(StringType())),
                        ]
                    ),
                ),
            ]
        )
        return schema

    def get_landing(self, spark, landing_file, data_interval_start):
        logging.info(f"Looking for JSON files at {landing_file}")
        landing_df = (
            spark.read.json(landing_file, self.get_field_list())
            .withColumn("ingestion_date", lit(data_interval_start))
            .withColumn("year_partition", date_format(lit(data_interval_start), "yyyy"))
            .withColumn("month_partition", date_format(lit(data_interval_start), "MM"))
            .withColumn("day_partition", date_format(lit(data_interval_start), "dd"))
        )
        return landing_df

    def evaluate(self, **kwargs):
        spark = self.get_spark()

        data_interval_start = kwargs["data_interval_start"]
        landing_file = f"{self.landing_path}/{data_interval_start}"
        landing_df = self.get_landing(spark, landing_file, data_interval_start)
        landing_df.printSchema()
        landing_df.show()

        if DeltaTable.isDeltaTable(spark, self.raw_path):
            logging.info("Merging landing files with raw")
            raw_df = DeltaTable.forPath(spark, self.raw_path)
            _ = (
                raw_df.alias("raw")
                .merge(
                    source=landing_df.alias("landing"),
                    condition=col("raw.id") == col("landing.id"),
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )
        else:
            logging.info("Raw files still not exists, creating a new one")
            _ = (
                landing_df.write.format("delta")
                .partitionBy("year_partition", "month_partition", "day_partition")
                .save(self.raw_path)
            )
