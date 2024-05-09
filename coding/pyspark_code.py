# filename: pyspark_code.py
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Pig-to-PySpark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data_dir = "/workspace/data"
output_dir = "/workspace/output"

if not os.path.isdir(data_dir):
    print(f"[ERROR] The 'data' directory does not exist at {data_dir}")
else:
    if not os.path.exists(os.path.join(data_dir, "sample1.csv")):
        print(f"[WARNING] File 'sample1.csv' does not exist at {os.path.join(data_dir, 'sample1.csv')}")
    else:
        os.chdir("/workspace")

        transactions = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(data_dir + "/sample1.csv") \
            .toDF("depStore", "date", "amount")

        high_value_transactions = transactions.filter(transactions.amount > 200)
        grouped_by_store = high_value_transactions.groupBy("depStore")
        sales_summary = grouped_by_store \
            .agg({"amount": "sum"}, {"amount": "avg"}) \
            .toDF("depStore", "total_sales", "average_sales")

        sales_summary.write \
            .format("csv") \
            .option("header", "true") \
            .mode('overwrite') \
            .save(output_dir)

        os.chdir("/workspace/pyspark_code")