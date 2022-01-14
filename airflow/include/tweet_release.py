import logging

from delta import configure_spark_with_delta_pip, DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, lit


class TweetRelease:
    def __init__(self, raw_path, trusted_path):
        self.raw_path = raw_path
        self.trusted_path = trusted_path

    def get_spark(self):
        builder = (
            SparkSession.builder.appName("tweet_release")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    def get_raw(self, ingestion_date):
        raw_df = (
            self.spark.read.format("delta")
            .load(self.raw_path)
            .where(
                (col("year_partition") == lit(ingestion_date.year))
                & (col("month_partition") == lit(ingestion_date.month))
                & (col("day_partition") == lit(ingestion_date.day))
            )
            .drop("watermark", "ingestion_date")
        )

        raw_df = (
            raw_df.withColumn("year_partition", date_format(col("created_at"), "yyyy"))
            .withColumn("month_partition", date_format(col("created_at"), "MM"))
            .withColumn("day_partition", date_format(col("created_at"), "dd"))
        )
        return raw_df

    def merge(self, raw_df):
        trusted_df = DeltaTable.forPath(self.spark, self.trusted_path)
        _ = (
            trusted_df.alias("trusted")
            .merge(
                source=raw_df.alias("raw"),
                condition=col("trusted.id") == col("raw.id"),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )

    def overwrite(self, raw_df):
        _ = (
            raw_df.write.format("delta")
            .partitionBy("year_partition", "month_partition", "day_partition")
            .save(self.trusted_path)
        )

    def is_delta(self, path):
        return DeltaTable.isDeltaTable(self.spark, path)

    def evaluate(self, **kwargs):
        self.spark = self.get_spark()

        ingestion_date = kwargs["data_interval_start"]
        raw_df = self.get_raw(ingestion_date)

        logging.info("Checking if Trusted already exists")
        if self.is_delta(self.trusted_path):
            logging.info("Merging raw files with trusted")
            self.merge(raw_df)
        else:
            logging.info("Trusted files still not exists, creating a new one")
            self.overwrite(raw_df)
