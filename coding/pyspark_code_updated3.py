# filename: pyspark_code_updated3.py
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Pig-to-PySpark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Use a relative file path to the input CSV file
abs_file_path = os.path.abspath("./data/sample1.csv")

# Load the data as before
input_data = spark.read.text(abs_file_path)

# Convert the loaded data into a DataFrame using RDD transformations
input_data_rdd = input_data.rdd
split_rows = input_data_rdd.map(lambda x: x.value.strip().split(","))
header = split_rows.first()
split_rows = split_rows.filter(lambda row: row != header)
transactions = spark.createDataFrame(
    split_rows,
    ["depStore", "date", "amount"],
)

# Process the transactions as before
# ... (no changes from previous code)