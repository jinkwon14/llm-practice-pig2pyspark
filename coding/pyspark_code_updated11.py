# filename: pyspark_code_updated11.py
from pyspark.sql import SparkSession
import os

cwd = "/code"
abs_file_path = "../data/sample1.csv"
if not abs_file_path.startswith("/"):
    abs_file_path = os.path.realpath(os.path.join(cwd, abs_file_path))
print("Absolute File Path:", abs_file_path)

spark = SparkSession.builder \
    .appName("Pig-to-PySpark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

input_data = spark.read.text(abs_file_path)
# ... (rest of the code remains unchanged from previous examples)