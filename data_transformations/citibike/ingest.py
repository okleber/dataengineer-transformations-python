import logging
from typing import List

from pyspark.sql import SparkSession


def sanitize_columns(columns: List[str]) -> List[str]:
    return [column.replace(" ", "_") for column in columns]


def run(spark: SparkSession, ingest_path: List[str], transformation_path: List[str]) -> None:
    for file in ingest_path:
        logging.info("Reading text file from: %s", file)
        input_df = spark.read.format("org.apache.spark.csv").option("header", True).csv(file)
        renamed_columns = sanitize_columns(input_df.columns)
        ref_df = input_df.toDF(*renamed_columns)
        ref_df.printSchema()
        ref_df.show()
        ref_df.write.parquet(transformation_path[0], mode="append")

