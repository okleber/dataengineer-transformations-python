import logging
import re

from pyspark.sql import SparkSession


def regex_pattern() -> re.Pattern:
    DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')
    return DELIMITER


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    delimiter = regex_pattern()
    logging.info("Reading text file from: %s", input_path)
    words = spark.sparkContext.textFile(input_path)\
        .flatMap(lambda line: delimiter
                 .split(line))
    word_count = words.map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    logging.info("Writing csv to directory: %s", output_path)
    df = spark.createDataFrame(word_count).toDF("word", "count")
    df.coalesce(1).write.csv(output_path, header=True)
