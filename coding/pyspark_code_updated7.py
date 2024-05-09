# filename: pyspark_code_updated7.py
from pyspark.sql import SparkSession
import os

abs_file_path = "/data/sample1.csv"
print("Absolute File Path:", abs_file_path)

spark = SparkSession.builder \
    .appName("Pig-to-PySpark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

input_data = spark.read.text(abs_file_path)
# ... (rest of the code remains unchanged from previous examples)