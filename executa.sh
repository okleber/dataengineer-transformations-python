#!/usr/bin/env bash
set -x
>project.log
poetry build
#poetry run spark-submit --master local --py-files dist/data_transformations-*.whl jobs/word_count.py resources/word_count/words.txt out/word_count
#poetry run spark-submit --master local --py-files dist/data_transformations-*.whl jobs/citibike_ingest.py resources/citibike/*.csv out/citibike
poetry run spark-submit --master local --py-files dist/data_transformations-*.whl jobs/citibike_distance_calculation.py out/citibike/*.parquet out/citibike_distance

cat project.log

