import logging

from pyspark.sql import SparkSession, DataFrame, Column
from typing import List
from pyspark.sql.functions import col
from haversine import haversine, Unit


def compute_row(row) -> Column:
    start = (col('start_station_latitude'), col('df.start_station_longitude'))
    end = (col('end_station_latitude'), col('df.end_station_longitude'))
    row.distance = col(str(haversine(start, end, unit=Unit.MILES)))
    return row.distance


def compute_distance(_spark: SparkSession, df: DataFrame) -> DataFrame:
    df.withColumn('distance', compute_row(df))
    return df


def run(spark: SparkSession, input_dataset_path: List[str], transformed_dataset_path: List[str]) -> None:
    for file in input_dataset_path:
        input_dataset = spark.read.parquet(file)
        input_dataset.show()
        dataset_with_distances = compute_distance(spark, input_dataset)
        dataset_with_distances.show()
        # dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
