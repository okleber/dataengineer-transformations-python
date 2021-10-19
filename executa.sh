#!/usr/bin/env bash
set -x
>project.log
poetry build
poetry run spark-submit --master local --py-files dist/data_transformations-*.whl jobs/word_count.py resources/word_count/words.txt out/word_count
cat project.log

